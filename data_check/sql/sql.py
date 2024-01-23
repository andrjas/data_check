from contextlib import contextmanager, suppress
from functools import cached_property
from os import path
from pathlib import Path
from time import sleep, time
from typing import Any, Dict, Iterator, List, Optional, Sequence, Union, cast

import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.engine.cursor import CursorResult
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.engine.row import Row
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.pool import SingletonThreadPool
from sqlalchemy.sql import text
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.sql.expression import bindparam

from data_check.config import DataCheckConfig

from ..exceptions import DataCheckException
from ..file_ops import print_csv, write_csv
from ..output import DataCheckOutput
from ..runner import DataCheckRunner
from .query_result import QueryResult
from .table_loader import TableLoader


class DataCheckSql:
    def __init__(
        self,
        connection: str,
        runner: Optional[DataCheckRunner] = None,
        output: Optional[DataCheckOutput] = None,
        config: Optional[DataCheckConfig] = None,
    ) -> None:
        self.connection = connection
        self.__connection: Optional[Connection] = None
        self.__engine: Optional[Engine] = None

        if output is None:
            output = DataCheckOutput()
        self.output = output

        if config is None:
            config = DataCheckConfig()
        self.config = config

        if runner is None:
            runner = DataCheckRunner(
                workers=1, output=self.output, use_process=self.config.use_process
            )
        self.runner = runner

    @cached_property
    def table_loader(self) -> TableLoader:
        """
        Lazy-load a TableLoader.
        """
        return TableLoader(self, self.output, self.config.default_load_mode)

    def get_db_params(self) -> Dict[str, Any]:
        """
        Return parameter specific to a database.
        """
        return {}  # no special parameters needed for now

    def keep_connection(self):
        # Do not keep the connection if runner has more than 1 workers.
        # Cannot pickle otherwise.
        return self.runner.workers == 1

    def get_engine(self, extra_params: Dict[str, Any] = {}) -> Engine:
        """
        Return the database engine for the connection.
        """
        # ignore unclosed SSLSocket ResourceWarning for Databricks
        import warnings

        warnings.filterwarnings(
            action="ignore", message="unclosed", category=ResourceWarning
        )

        if self.__engine is None:
            _engine = create_engine(
                path.expandvars(self.connection),
                **{**self.get_db_params(), **extra_params},
                poolclass=SingletonThreadPool,
            )
            self.post_get_engine_hook(_engine)
            if self.keep_connection():
                self.__engine = _engine
            else:
                return _engine
        return self.__engine

    def post_get_engine_hook(self, engine: Engine):
        pass

    @contextmanager
    def conn(self) -> Iterator[Connection]:
        with self.get_engine().connect() as c:
            yield c
            c.commit()

    def disconnect(self):
        if self.__connection:
            with suppress(ProgrammingError):
                self.__connection.close()
            self.__connection = None
        if self.__engine:
            with suppress(ProgrammingError):
                self.__engine.dispose(close=False)
            self.__engine = None

    def __del__(self):
        # __del__ is not always called when the object gets deleted,
        # but when it does, we should disconnect cleanly.
        self.disconnect()

    @property
    def dialect(self) -> str:
        return self.get_engine().dialect.name

    def run_query(self, query: str, params: Dict[str, Any] = {}) -> pd.DataFrame:
        """
        Run a query on the database and return a Pandas DataFrame with the result.
        """
        return self.run_query_with_result(query, params=params).df

    def use_parameters(self) -> bool:
        if self.dialect == "databricks":
            # cannot use bindparams due to https://github.com/databricks/databricks-sql-python/pull/267
            return False
        return True

    def _bindparams(self, query: str) -> TextClause:
        sql = text(query)
        if not self.use_parameters():
            return sql
        for bp in sql._bindparams.keys():
            sql = sql.bindparams(bindparam(bp, expanding=True))
        return sql

    def run_query_with_result(
        self, query: str, params: Dict[str, Any] = {}
    ) -> QueryResult:
        if not self.connection:
            raise DataCheckException(f"undefined connection: {self.connection}")
        sql = self._bindparams(query)
        with self.conn() as c:
            if self.use_parameters():
                result = QueryResult(query, c.execute(sql, parameters=params))
            else:
                result = QueryResult(query, c.execute(sql))
        return result

    def _try_connect(self, engine) -> bool:
        try:
            engine.connect()
            return True
        except Exception as e:  # noqa E722
            self.output.print_exception(e)
            return False

    def test_connection(
        self,
        wait=False,
        timeout=DataCheckConfig.wait_timeout,
        retry=DataCheckConfig.wait_retry,
    ) -> bool:
        """
        Returns True if we can connect to the database.
        If "wait" is true, we retry each "retry" second, for "timeout" seconds.
        Mainly for integration tests.
        """
        success_msg = "connecting succeeded"
        engine = self.get_engine(extra_params={"pool_pre_ping": True})
        if wait:
            timeout_time = time() + timeout
            while time() < timeout_time:
                if self._try_connect(engine):
                    self.output.print(success_msg)
                    return True
                else:
                    sleep(retry)
                    self.output.print("retry")
        else:
            if self._try_connect(engine):
                self.output.print(success_msg)
                return True
        self.output.print("connecting failed")
        return False

    def run_sql(
        self,
        query: str,
        output: Union[str, Path] = "",
        params: Dict[str, Any] = {},
        base_path: Path = Path(),
        sort_output: bool = False,
    ) -> Union[bool, Sequence[Row]]:
        """
        Runs a SQL statement. If it is a query, a list of the rows is returned, otherwise a boolean is returned indicating success or failure of the statement.
        If output is given, the result of the query is written to the file relative to base_path.
        """
        sq_text = self._bindparams(query)
        with self.conn() as connection:
            result: CursorResult
            if self.use_parameters():
                result = connection.execute(
                    sq_text.execution_options(), parameters=params
                )
            else:
                result = connection.execute(sq_text.execution_options())
        try:
            res: Sequence[Row] = result.fetchall()
            columns: List[str] = list(result.keys())
            df = pd.DataFrame(data=res, columns=columns)
            if output:
                write_csv(
                    df, output=output, base_path=base_path, sort_output=sort_output
                )
            else:
                print_csv(df, self.output.print)
            if self.dialect == "databricks":
                # Databricks returns an empty list and a single column 'Result' if the query is a DML/DDL.
                # That's why we convert it to True in this case to indicate successful execution.
                if res == [] and columns == ["Result"]:
                    return True
            return res
        except Exception:
            return bool(result)

    @property
    def inspector(self) -> Inspector:
        return cast(Inspector, inspect(self.get_engine()))

    def get_truncate_table_statement(self, table_name: str) -> str:
        return f"TRUNCATE TABLE {table_name}"

    def get_drop_table_statement(self, table_name: str) -> str:
        return f"DROP TABLE {table_name}"

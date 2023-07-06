from contextlib import contextmanager, suppress
from functools import cached_property
from os import path
from pathlib import Path
from time import sleep, time
from typing import Any, Dict, List, Optional, Union, cast

import pandas as pd
from sqlalchemy import create_engine, event, inspect
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
        if self.__engine is None:
            _engine = create_engine(
                path.expandvars(self.connection),
                **{**self.get_db_params(), **extra_params},
                poolclass=SingletonThreadPool,
            )
            self.register_setinputsizes_event(_engine)
            if self.keep_connection():
                self.__engine = _engine
            else:
                return _engine
        return self.__engine

    def register_setinputsizes_event(self, engine: Engine):
        if engine.dialect.name == "oracle":
            # do not use CLOB for loading strings (and large decimals)
            # https://docs.sqlalchemy.org/en/14/dialects/oracle.html#example-2-remove-all-bindings-to-clob
            try:
                from cx_Oracle import CLOB  # type: ignore
            except ImportError:
                CLOB = None

            @event.listens_for(engine, "do_setinputsizes")
            def _remove_clob(inputsizes, cursor, statement, parameters, context):
                _ = cursor
                _ = statement
                _ = parameters
                _ = context
                for bindparam, dbapitype in list(inputsizes.items()):
                    if dbapitype is CLOB:
                        del inputsizes[bindparam]

    @contextmanager
    def conn(self) -> Connection:
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

    @staticmethod
    def _bindparams(query: str) -> TextClause:
        sql = cast(TextClause, text(query))
        for bp in sql._bindparams.keys():  # type: ignore
            sql = sql.bindparams(bindparam(bp, expanding=True))
        return sql

    def run_query_with_result(
        self, query: str, params: Dict[str, Any] = {}
    ) -> QueryResult:
        if not self.connection:
            raise DataCheckException(f"undefined connection: {self.connection}")
        sql = self._bindparams(query)
        with self.conn() as c:
            result = QueryResult(query, c.execute(sql, parameters=params))
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
    ):
        """
        Runs a SQL statement. If it is a query, a list of the rows is returned, otherwise a boolean is returned indicating success or failure of the statement.
        If output is given, the result of the query is written to the file relative to base_path.
        """
        sq_text = self._bindparams(query)
        with self.conn() as connection:
            result: CursorResult = connection.execute(
                sq_text.execution_options(), parameters=params
            )
        try:
            res: List[Row] = result.fetchall()
            columns: List[str] = list(result.keys())
            df = pd.DataFrame(data=res, columns=columns)
            if output:
                write_csv(df, output=output, base_path=base_path)
            else:
                print_csv(df, self.output.print)
            return res
        except Exception:
            return bool(result)

    @property
    def inspector(self) -> Inspector:
        return cast(Inspector, inspect(self.get_engine()))

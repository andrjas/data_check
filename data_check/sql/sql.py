from typing import Dict, Any, Optional, Union, List
from os import path
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.engine.row import Row
from sqlalchemy.engine.cursor import CursorResult
from sqlalchemy.sql import text
from sqlalchemy.sql.expression import bindparam
import pandas as pd
from pathlib import Path


from ..exceptions import DataCheckException
from ..output import DataCheckOutput
from ..io import print_csv, write_csv
from ..runner import DataCheckRunner
from .table_loader import TableLoader
from .query_result import QueryResult


class DataCheckSql:
    def __init__(
        self,
        connection: str,
        runner: Optional[DataCheckRunner] = None,
        output: Optional[DataCheckOutput] = None,
    ) -> None:
        self.connection = connection
        self.__connection: Optional[Connection] = None
        self.__engine: Optional[Engine] = None

        if output is None:
            output = DataCheckOutput()
        self.output = output

        if runner is None:
            runner = DataCheckRunner(workers=1, output=self.output)
        self.runner = runner

        self._table_loader: Optional[TableLoader] = None

    @property
    def table_loader(self) -> TableLoader:
        """
        Lazy-load a TableLoader.
        """
        if self._table_loader is None:
            self._table_loader = TableLoader(self, self.output)
        return self._table_loader

    def get_db_params(self) -> Dict[str, Any]:
        """
        Return parameter specific to a database.
        """
        return {}  # no special parameters needed for now

    def _keep_connection(self):
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
            )
            if self._keep_connection():
                self.__engine = _engine
            else:
                return _engine
        return self.__engine

    def get_connection(self) -> Connection:
        if self.__connection is None:
            _connection = self.get_engine().connect()
            if self._keep_connection():
                self.__connection = _connection
            else:
                return _connection
        return self.__connection

    @property
    def dialect(self) -> str:
        return self.get_connection().dialect.name

    def run_query(self, query: str, params: Dict[str, Any] = {}) -> pd.DataFrame:
        """
        Run a query on the database and return a Pandas DataFrame with the result.
        """
        return self.run_query_with_result(query, params=params).df

    def run_query_with_result(
        self, query: str, params: Dict[str, Any] = {}
    ) -> QueryResult:
        if not self.connection:
            raise DataCheckException(f"undefined connection: {self.connection}")
        sql = text(query)
        for bp in sql._bindparams.keys():
            sql = sql.bindparams(bindparam(bp, expanding=True))

        result = QueryResult(query, self.get_connection().execute(sql, **params))
        return result

    def test_connection(self) -> bool:
        """
        Returns True if we can connect to the database.
        Mainly for integration tests.
        """
        engine = self.get_engine(extra_params={"pool_pre_ping": True})
        try:
            engine.connect()
            self.output.print("connecting succeeded")
            return True
        except:  # noqa E722
            self.output.print("connecting failed")
            return False

    def run_sql(
        self, query: str, output: Union[str, Path] = "", base_path: Path = Path(".")
    ):
        sq_text = text(query)
        result: CursorResult = self.get_connection().execute(
            sq_text.execution_options(autocommit=True)
        )
        try:
            res: List[Row] = result.fetchall()
            columns: List[str] = result.keys()
            df = pd.DataFrame(data=res, columns=columns)
            print_csv(df, self.output.print)
            write_csv(df, output=output, base_path=base_path)
            return res
        except Exception:
            return bool(result)

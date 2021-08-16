from typing import Dict, Any, Optional, Union
from os import path
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.sql import text
import pandas as pd
from pathlib import Path


from ..exceptions import DataCheckException
from ..io import print_csv, write_csv
from ..runner import DataCheckRunner
from .table_loader import TableLoader


class DataCheckSql:
    def __init__(
        self, connection: str, runner: DataCheckRunner = DataCheckRunner(workers=1)
    ) -> None:
        self.connection = connection
        self.__connection: Optional[Connection] = None
        self.__engine: Optional[Engine] = None
        self.runner = runner
        self._table_loader: Optional[TableLoader] = None

    @property
    def table_loader(self) -> TableLoader:
        """
        Lazy-load a TableLoader.
        """
        if self._table_loader is None:
            self._table_loader = TableLoader(self)
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

    def run_query(self, query: str) -> pd.DataFrame:
        """
        Run a query on the database and return a Pandas DataFrame with the result.
        """
        if not self.connection:
            raise DataCheckException(f"undefined connection: {self.connection}")
        return pd.read_sql_query(query, self.get_connection())

    def test_connection(self) -> bool:
        """
        Returns True if we can connect to the database.
        Mainly for integration tests.
        """
        engine = self.get_engine(extra_params={"pool_pre_ping": True})
        try:
            engine.connect()
            return True
        except:  # noqa E722
            return False

    def run_sql(
        self, query: str, output: Union[str, Path] = "", base_path: Path = Path(".")
    ):
        sq_text = text(query)
        result = self.get_connection().execute(
            sq_text.execution_options(autocommit=True)
        )
        try:
            res = result.fetchall()
            df = pd.DataFrame(data=res, columns=result.keys())
            print_csv(df)
            write_csv(df, output=output, base_path=base_path)
            return res
        except:
            return bool(result)

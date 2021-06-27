
from typing import List, Dict
from os import linesep, path
from sqlalchemy import create_engine
import pandas as pd


from .data_check_exception import DataCheckException


class DataCheckSql:
    def __init__(self, connection: str) -> None:
        self.connection = connection

    def get_db_params(self) -> Dict:
        """
        Return parameter specific to a database.
        """
        return {}  # no special parameters needed for now

    def get_engine(self, extra_params={}):
        """
        Return the database engine for the connection.
        """
        return create_engine(
            path.expandvars(self.connection), **{**self.get_db_params(), **extra_params}
        )

    def run_query(self, query: str) -> pd.DataFrame:
        """
        Run a query on the database and return a Pandas DataFrame with the result.
        """
        if not self.connection:
            raise DataCheckException(f"undefined connection: {self.connection}")
        with self.get_engine().connect() as connection:
            return pd.read_sql_query(query, connection)

    def test_connection(self) -> bool:
        """
        Returns True if we can connect to the database.
        Mainly for integration tests.
        """
        engine = self.get_engine(extra_params={"pool_pre_ping": True})
        try:
            engine.connect()
            return True
        except:
            return False

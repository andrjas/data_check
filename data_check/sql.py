from typing import Dict, Any, Optional, Tuple, List
from os import path
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.sql import text
import pandas as pd
from enum import Enum
import warnings
from pathlib import Path


from .exceptions import DataCheckException
from .io import expand_files, read_csv
from .runner import DataCheckRunner
from .config import DataCheckConfig


class LoadMethod(Enum):
    TRUNCATE = 1
    APPEND = 2
    REPLACE = 3

    @staticmethod
    def from_string(method_name: str):
        if method_name == "truncate":
            return LoadMethod.TRUNCATE
        elif method_name == "append":
            return LoadMethod.APPEND
        elif method_name == "replace":
            return LoadMethod.REPLACE
        else:
            raise DataCheckException(f"unknown load method: {method_name}")


class DataCheckSql:
    def __init__(
        self, connection: str, runner: DataCheckRunner = DataCheckRunner(workers=1)
    ) -> None:
        self.connection = connection
        self.__connection: Optional[Connection] = None
        self.__engine: Optional[Engine] = None
        self.runner = runner

    def get_db_params(self) -> Dict[str, Any]:
        """
        Return parameter specific to a database.
        """
        return {}  # no special parameters needed for now

    def get_engine(self, extra_params: Dict[str, Any] = {}) -> Engine:
        """
        Return the database engine for the connection.
        """
        if self.__engine is None:
            self.__engine = create_engine(
                path.expandvars(self.connection),
                **{**self.get_db_params(), **extra_params},
            )
        return self.__engine

    def get_connection(self):
        if self.__connection is None:
            self.__connection = self.get_engine().connect()
        return self.__connection

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

    def _prepare_table_for_load(self, table_name: str, load_method: LoadMethod):
        if load_method == LoadMethod.TRUNCATE:
            schema, name = self._parse_table_name(table_name)
            inspector = inspect(self.get_connection())
            if inspector.has_table(table_name=name, schema=schema):
                self.get_connection().execute(
                    text(f"DELETE FROM {table_name}").execution_options(autocommit=True)
                )

    def _parse_table_name(self, table_name: str) -> Tuple[Optional[str], str]:
        """Parses the table_name and returns the schema and table_name.
        Returns None for schema if table_name is a simple table without schema.
        """
        if "." in table_name:
            schema, name = table_name.split(".", maxsplit=1)
            return (schema, name)
        else:
            return (None, table_name)

    @staticmethod
    def _load_method_to_pandas_if_exists(load_method: LoadMethod) -> str:
        if load_method == LoadMethod.TRUNCATE:
            return "append"  # for truncate we use apend but with some preparations
        elif load_method == LoadMethod.APPEND:
            return "append"
        elif load_method == LoadMethod.REPLACE:
            return "replace"

    def load_table(self, table_name: str, data: pd.DataFrame, load_method: LoadMethod):
        self._prepare_table_for_load(table_name, load_method)
        if_exists = self._load_method_to_pandas_if_exists(load_method=load_method)
        schema, name = self._parse_table_name(table_name)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # ignore SADeprecationWarning
            data.to_sql(
                name=name,
                schema=schema,
                con=self.get_connection(),
                if_exists=if_exists,
                index=False,
            )

    def load_table_from_csv_file(
        self, table_name: str, csv_file: Path, load_method: LoadMethod
    ):
        data = read_csv(csv_file=csv_file)
        self.load_table(table_name=table_name, data=data, load_method=load_method)
        print(f"table {table_name} loaded from {csv_file}")

    def load_tables_from_files(
        self,
        files: List[Path],
        load_method: LoadMethod,
    ):
        csv_files = expand_files(files, extension=".csv")
        parameters = [
            {"table_name": f.stem, "csv_file": f, "load_method": load_method}
            for f in csv_files
        ]
        self.runner.run_any(
            run_method=self.load_table_from_csv_file, parameters=parameters
        )

    @staticmethod
    def load_method_from_string(lm_str: str) -> LoadMethod:
        return LoadMethod.from_string(lm_str)

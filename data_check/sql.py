from typing import Dict, Any, Optional, Tuple, List, Union
from os import path
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.sql import text
from sqlalchemy.exc import NoSuchTableError, OperationalError
import pandas as pd
from enum import Enum
import warnings
from pathlib import Path
import datetime


from .exceptions import DataCheckException
from .io import expand_files, read_csv
from .runner import DataCheckRunner


class LoadMode(Enum):
    TRUNCATE = 1
    APPEND = 2
    REPLACE = 3

    @staticmethod
    def from_string(mode_name: str):
        if mode_name == "truncate":
            return LoadMode.TRUNCATE
        elif mode_name == "append":
            return LoadMode.APPEND
        elif mode_name == "replace":
            return LoadMode.REPLACE
        else:
            raise DataCheckException(f"unknown load mode: {mode_name}")


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

    def table_exists(self, table_name: str, schema: str):
        inspector = inspect(self.get_connection())
        return inspector.has_table(table_name=table_name, schema=schema)

    def drop_table_if_exists(self, table_name: str, schema: str):
        if self.table_exists(table_name, schema):
            if schema:
                drop_stmt = f"DROP TABLE {schema}.{table_name}"
            else:
                drop_stmt = f"DROP TABLE {table_name}"
            self.get_connection().execute(
                text(drop_stmt).execution_options(autocommit=True)
            )

    def _prepare_table_for_load(self, table_name: str, load_mode: LoadMode):
        if load_mode == LoadMode.TRUNCATE:
            schema, name = self._parse_table_name(table_name)
            inspector = inspect(self.get_connection())
            if inspector.has_table(table_name=name, schema=schema):
                self.get_connection().execute(
                    text(f"DELETE FROM {table_name}").execution_options(autocommit=True)
                )
        elif load_mode == LoadMode.REPLACE:
            # Pandas and SQLAlchemy seem to have problems using if_exists="replace"
            # at least in SQLite. That's why we drop the tables here.
            schema, name = self._parse_table_name(table_name)
            self.drop_table_if_exists(name, schema)

    def _parse_table_name(self, table_name: str) -> Tuple[Optional[str], str]:
        """Parses the table_name and returns the schema and table_name.
        Returns None for schema if table_name is a simple table without schema.
        The schema and table name will always be upper cased.
        """
        if "." in table_name:
            schema, name = table_name.lower().split(".", maxsplit=1)
            return (schema, name)
        else:
            return (None, table_name.lower())

    @staticmethod
    def _load_mode_to_pandas_if_exists(load_mode: LoadMode) -> str:
        # always use "append" since we prepare the tables before loading
        return "append"

    def load_table(
        self, table_name: str, data: pd.DataFrame, load_mode: LoadMode, dtype=None
    ):
        self._prepare_table_for_load(table_name, load_mode)
        if_exists = self._load_mode_to_pandas_if_exists(load_mode=load_mode)
        schema, name = self._parse_table_name(table_name)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # ignore SADeprecationWarning
            data.to_sql(
                name=name,
                schema=schema,
                con=self.get_connection(),
                if_exists=if_exists,
                index=False,
                dtype=dtype,
            )
            return True

    def get_date_columns(self, table_name: str):
        schema, name = self._parse_table_name(table_name)
        date_column_types = (
            datetime.datetime,
            datetime.date,
            datetime.time,
        )
        try:
            inspector = inspect(self.get_connection())
            columns = inspector.get_columns(name, schema=schema)
            return {
                c["name"]: c["type"]
                for c in columns
                if c["type"].python_type in date_column_types
            }
        except (NoSuchTableError, OperationalError):
            # Python 3.6 might trow an OperationalError
            return {}

    def load_table_from_csv_file(
        self,
        table: str,
        file: Path,
        load_mode: Union[str, LoadMode] = LoadMode.TRUNCATE,
        base_path: Path = Path("."),
    ):
        if isinstance(load_mode, str):
            load_mode = self.load_mode_from_string(load_mode)
        rel_file = base_path / file
        date_columns = self.get_date_columns(table)
        date_column_names = list(date_columns.keys())
        data = read_csv(csv_file=rel_file, parse_dates=date_column_names)
        result = self.load_table(
            table_name=table,
            data=data,
            load_mode=load_mode,
            dtype=date_columns,
        )
        if result:
            print(f"table {table} loaded from {rel_file}")
        else:
            print(f"loading table {table} from {rel_file} failed")
        return result

    def load_tables_from_files(
        self,
        files: List[Path],
        load_mode: Union[str, LoadMode] = LoadMode.TRUNCATE,
        base_path: Path = Path("."),
    ):
        if isinstance(load_mode, str):
            load_mode = self.load_mode_from_string(load_mode)
        csv_files = expand_files(files, extension=".csv", base_path=base_path)
        parameters = [
            {"table": f.stem, "file": f, "load_mode": load_mode} for f in csv_files
        ]
        results = self.runner.run_any(
            run_method=self.load_table_from_csv_file, parameters=parameters
        )
        return all(results)

    @staticmethod
    def load_mode_from_string(lm_str: str) -> LoadMode:
        return LoadMode.from_string(lm_str)

    def run_sql(self, sql_text: str):
        sq_text = text(sql_text)
        result = self.get_connection().execute(
            sq_text.execution_options(autocommit=True)
        )
        return result

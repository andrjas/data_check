from __future__ import annotations
from typing import Optional, Tuple, List, Union, Any, Dict, TYPE_CHECKING
from sqlalchemy import inspect
from sqlalchemy.sql import text
from sqlalchemy.exc import NoSuchTableError, OperationalError
import pandas as pd
import warnings
from pathlib import Path
import datetime
from dataclasses import dataclass

if TYPE_CHECKING:
    from data_check.sql import DataCheckSql
    from data_check.output import DataCheckOutput

from ..io import expand_files, read_csv
from .load_mode import LoadMode


# some data types that need special handling
@dataclass
class ColumnInfo:
    dtypes: Dict[Any, Any]
    date_columns: Dict[Any, Any]
    date_column_names: List[str]
    string_columns: Dict[Any, Any]
    string_column_names: List[str]


class TableLoader:
    """
    Helper class that implements the methods to load a table from a CSV file.
    """

    def __init__(self, sql: DataCheckSql, output: DataCheckOutput):
        self.sql = sql
        self.output = output

    def table_exists(self, table_name: str, schema: Optional[str]):
        inspector = inspect(self.sql.get_connection())
        return inspector.has_table(table_name=table_name, schema=schema)

    def drop_table_if_exists(self, table_name: str, schema: Optional[str]):
        if self.table_exists(table_name, schema):
            if schema:
                drop_stmt = f"DROP TABLE {schema}.{table_name}"
            else:
                drop_stmt = f"DROP TABLE {table_name}"
            self.sql.get_connection().execute(
                text(drop_stmt).execution_options(autocommit=True)
            )

    def _truncate_statement(self, table_name: str) -> str:
        if self.sql.dialect == "sqlite":
            return f"DELETE FROM {table_name}"
        else:
            return f"TRUNCATE TABLE {table_name}"

    def _prepare_table_for_load(self, table_name: str, load_mode: LoadMode):
        if load_mode == LoadMode.TRUNCATE:
            schema, name = self._parse_table_name(table_name)
            inspector = inspect(self.sql.get_connection())
            if inspector.has_table(table_name=name, schema=schema):
                self.sql.get_connection().execute(
                    text(self._truncate_statement(table_name)).execution_options(
                        autocommit=True
                    )
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
                con=self.sql.get_connection(),
                if_exists=if_exists,
                index=False,
                dtype=dtype,
            )
            return True

    def get_column_types(self, table_name: str):
        schema, name = self._parse_table_name(table_name)
        try:
            inspector = inspect(self.sql.get_connection())
            columns = inspector.get_columns(name, schema=schema)
            return {c["name"]: c["type"] for c in columns}
        except NoSuchTableError:
            return {}

    def get_date_columns(self, table_name: str):
        date_column_types = (
            datetime.datetime,
            datetime.date,
            datetime.time,
        )
        col_types = self.get_column_types(table_name)
        return {
            k: col_types[k]
            for k in col_types.keys()
            if col_types[k].python_type in date_column_types
        }

    def get_string_columns(self, table_name: str):
        col_types = self.get_column_types(table_name)
        return {
            k: col_types[k] for k in col_types.keys() if col_types[k].python_type == str
        }

    def get_column_info(self, table_name: str) -> ColumnInfo:
        date_columns = self.get_date_columns(table_name)
        date_column_names = list(date_columns.keys())
        string_columns = self.get_string_columns(table_name)
        string_column_names = list(string_columns.keys())
        dtypes = dict(date_columns, **string_columns)
        column_info = ColumnInfo(
            dtypes=dtypes,
            date_columns=date_columns,
            date_column_names=date_column_names,
            string_columns=string_columns,
            string_column_names=string_column_names,
        )
        return column_info

    def load_table_from_file(
        self,
        table: str,
        file: Path,
        load_mode: Union[str, LoadMode] = LoadMode.TRUNCATE,
        base_path: Path = Path("."),
    ):
        if isinstance(load_mode, str):
            load_mode = self.load_mode_from_string(load_mode)
        rel_file = base_path / file
        column_info = self.get_column_info(table)
        data = self.load_df_from_file(rel_file, column_info)

        result = self.load_table(
            table_name=table,
            data=data,
            load_mode=load_mode,
            dtype=column_info.dtypes,
        )
        if result:
            self.output.print(f"table {table} loaded from {rel_file}")
        else:
            self.output.print(f"loading table {table} from {rel_file} failed")
        return result

    def load_df_from_file(self, file: Path, column_info: ColumnInfo) -> pd.DataFrame:
        if file.suffix.lower() == ".csv":
            data = read_csv(
                csv_file=file,
                parse_dates=column_info.date_column_names,
                string_columns=column_info.string_column_names,
            )
        elif file.suffix.lower() == ".xlsx":
            data = pd.read_excel(
                file, sheet_name=0, header=0, engine="openpyxl", dtype="object"
            )
        else:
            raise Exception(f"file type unsupported: {file.suffix.lower()}")
        return data

    def load_tables_from_files(
        self,
        files: List[Path],
        load_mode: Union[str, LoadMode] = LoadMode.TRUNCATE,
        base_path: Path = Path("."),
    ):
        if isinstance(load_mode, str):
            load_mode = self.load_mode_from_string(load_mode)
        flat_files = expand_files(
            files, extension=[".csv", ".xlsx"], base_path=base_path
        )
        parameters = [
            {"table": f.stem, "file": f, "load_mode": load_mode} for f in flat_files
        ]
        results = self.sql.runner.run_any(
            run_method=self.load_table_from_file, parameters=parameters
        )
        return all(results)

    @staticmethod
    def load_mode_from_string(lm_str: str) -> LoadMode:
        return LoadMode.from_string(lm_str)

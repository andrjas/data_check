from __future__ import annotations
from sqlalchemy import inspect, Table, MetaData
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import NoSuchTableError
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast, Dict, List, Tuple, Any, Optional
import datetime

from functools import cached_property

if TYPE_CHECKING:
    from data_check.sql import DataCheckSql


# some data types that need special handling
@dataclass
class ColumnInfo:
    dtypes: Dict[Any, Any]
    date_columns: Dict[Any, Any]
    date_column_names: List[str]
    string_columns: Dict[Any, Any]
    string_column_names: List[str]


class TableInfo:
    def __init__(self, sql: DataCheckSql):
        self.sql = sql

    @property
    def inspector(self) -> Inspector:
        if self.sql.keep_connection():
            return self.cached_inspector
        return cast(Inspector, inspect(self.sql.get_engine()))

    @cached_property
    def cached_inspector(self) -> Inspector:
        return cast(Inspector, inspect(self.sql.get_engine()))

    def table_exists(self, table_name: str, schema: Optional[str]) -> bool:
        return self.inspector.has_table(table_name=table_name, schema=schema)

    def get_column_types(self, table_name: str):
        schema, name = self.parse_table_name(table_name)
        try:
            columns = self.inspector.get_columns(name, schema=schema)
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

    def parse_table_name(self, table_name: str) -> Tuple[Optional[str], str]:
        """Parses the table_name and returns the schema and table_name.
        Returns None for schema if table_name is a simple table without schema.
        The schema and table name will always be upper cased.
        """
        if "." in table_name:
            schema, name = table_name.lower().split(".", maxsplit=1)
            return (schema, name)
        return (None, table_name.lower())

    def get_primary_keys(self, table_name: str, schema: Optional[str]) -> List[str]:
        pk_constraint = self.inspector.get_pk_constraint(
            table_name=table_name, schema=schema
        )
        return pk_constraint["constrained_columns"]

    def get_table(self, table_name: str, schema: Optional[str]) -> Table:
        engine = self.sql.get_engine()
        metadata = MetaData(engine)
        table = Table(
            table_name,
            metadata,
            autoload_with=engine,
            schema=schema,
        )
        return table

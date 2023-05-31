from __future__ import annotations

import datetime
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Dict, List, Optional

from sqlalchemy import MetaData
from sqlalchemy import Table as SQLTable
from sqlalchemy import text
from sqlalchemy.exc import NoSuchTableError

from data_check.sql.sql import DataCheckSql


# some data types that need special handling
@dataclass
class ColumnInfo:
    dtypes: Dict[Any, Any]
    date_columns: Dict[Any, Any]
    date_column_names: List[str]
    string_columns: Dict[Any, Any]
    string_column_names: List[str]


class Table:
    def __init__(self, sql: DataCheckSql, name: str, schema: Optional[str] = None):
        self.sql = sql
        self.name = name
        self.schema = schema

    def drop_if_exists(self):
        if self.exists():
            drop_stmt = f"DROP TABLE {self.full_name}"
            with self.sql.conn() as connection:
                connection.execute(text(drop_stmt))

    def exists(self) -> bool:
        return self.sql.inspector.has_table(table_name=self.name, schema=self.schema)

    @property
    def full_name(self) -> str:
        if self.schema:
            return f"{self.schema}.{self.name}"
        return self.name

    def __str__(self) -> str:
        return self.full_name

    def _truncate_statement(self) -> str:
        if self.sql.dialect == "sqlite":
            return f"DELETE FROM {self.full_name}"
        return f"TRUNCATE TABLE {self.full_name}"

    def truncate_if_exists(self):
        if self.exists():
            with self.sql.conn() as connection:
                connection.execute(text(self._truncate_statement()))

    @staticmethod
    def from_table_name(sql: DataCheckSql, table_name: str) -> Table:
        if "." in table_name:
            schema, name = table_name.lower().split(".", maxsplit=1)
        else:
            schema, name = (None, table_name.lower())
        return Table(sql, name, schema)

    @cached_property
    def primary_keys(self) -> List[str]:
        pk_constraint = self.sql.inspector.get_pk_constraint(
            table_name=self.name, schema=self.schema
        )
        return pk_constraint["constrained_columns"]

    @cached_property
    def sql_table(self) -> SQLTable:
        metadata = MetaData()
        with self.sql.conn() as c:
            table = SQLTable(
                self.name,
                metadata,
                autoload_with=c,
                schema=self.schema,
            )
            return table

    @cached_property
    def columns(self):
        return self.sql.inspector.get_columns(self.name, schema=self.schema)

    @cached_property
    def column_types(self):
        try:
            return {c["name"]: c["type"] for c in self.columns}
        except NoSuchTableError:
            return {}

    @cached_property
    def date_columns(self):
        date_column_types = (
            datetime.datetime,
            datetime.date,
            datetime.time,
        )
        col_types = self.column_types
        return {
            k: col_types[k]
            for k in col_types.keys()
            if col_types[k].python_type in date_column_types
        }

    @cached_property
    def string_columns(self):
        col_types = self.column_types
        return {
            k: col_types[k] for k in col_types.keys() if col_types[k].python_type == str
        }

    @cached_property
    def column_info(self) -> ColumnInfo:
        date_column_names = list(self.date_columns.keys())
        string_column_names = list(self.string_columns.keys())
        dtypes = dict(self.date_columns, **self.string_columns)
        column_info = ColumnInfo(
            dtypes=dtypes,
            date_columns=self.date_columns,
            date_column_names=date_column_names,
            string_columns=self.string_columns,
            string_column_names=string_column_names,
        )
        return column_info

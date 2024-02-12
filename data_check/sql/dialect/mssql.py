from functools import cached_property

from sqlalchemy.engine import Connection
from sqlalchemy.sql import text

from data_check.sql import DataCheckSql
from data_check.sql.table import Table
from data_check.sql.table_loader import TableLoader


class TableLoaderMSSQL(TableLoader):
    def pre_insert(self, connection: Connection, table: Table):
        # When appending/upserting data into a table with identity columns,
        # we need to enable IDENTITY_INSERT to allow inserting explicit values
        # into these columns.
        if table.exists() and table.sql_table.primary_key:
            connection.execute(text(f"SET IDENTITY_INSERT {table} ON"))


class DataCheckSqlMSSQL(DataCheckSql):
    @cached_property
    def table_loader(self) -> TableLoader:
        return TableLoaderMSSQL(self, self.output, self.config.default_load_mode)

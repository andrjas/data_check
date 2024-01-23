from data_check.sql import DataCheckSql


class DataCheckSqlSQLite(DataCheckSql):
    def get_truncate_table_statement(self, table_name: str) -> str:
        return f"DELETE FROM {table_name}"

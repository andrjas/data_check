from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import ArgumentError

from .load_mode import LoadMode  # noqa: F401
from .sql import DataCheckSql
from .table import (
    ColumnInfo,  # noqa: F401
    Table,  # noqa: F401
)

if TYPE_CHECKING:
    from ..config import DataCheckConfig
    from ..output import DataCheckOutput
    from ..runner import DataCheckRunner


def get_dialect_for_connection(connection: str) -> str:
    try:
        url = make_url(connection)
        return url.get_dialect().name
    except ArgumentError:
        return ""


def get_sql(
    connection: str,
    runner: Optional[DataCheckRunner] = None,
    output: Optional[DataCheckOutput] = None,
    config: Optional[DataCheckConfig] = None,
) -> DataCheckSql:
    """Returns a specialized dialect for the given connection
    or DataCheckSql as fallback.
    """
    dialect = get_dialect_for_connection(connection)
    from .dialect import (
        DataCheckSqlDatabricks,
        DataCheckSqlDuckDB,
        DataCheckSqlMSSQL,
        DataCheckSqlMySQL,
        DataCheckSqlOracle,
        DataCheckSqlPostgreSQL,
        DataCheckSqlSQLite,
    )

    dialect_cls = {
        "duckdb": DataCheckSqlDuckDB,
        "mssql": DataCheckSqlMSSQL,
        "mysql": DataCheckSqlMySQL,
        "oracle": DataCheckSqlOracle,
        "postgresql": DataCheckSqlPostgreSQL,
        "sqlite": DataCheckSqlSQLite,
        "databricks": DataCheckSqlDatabricks,
    }.get(dialect, DataCheckSql)

    return dialect_cls(
        connection=connection,
        runner=runner,
        output=output,
        config=config,
    )

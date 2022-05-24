import pytest

from data_check.sql import DataCheckSql
from data_check.sql.table_info import TableInfo


@pytest.fixture
def table_info(sql: DataCheckSql):
    return sql.table_info


def test_parse_table_name_with_schema(table_info: TableInfo):
    tn = "a.b"
    schema, name = table_info.parse_table_name(tn)
    assert schema == "a"
    assert name == "b"


def test_parse_table_name(table_info: TableInfo):
    tn = "b"
    schema, name = table_info.parse_table_name(tn)
    assert schema is None
    assert name == "b"


def test_parse_table_name_dot(table_info: TableInfo):
    tn = "."
    schema, name = table_info.parse_table_name(tn)
    assert schema == ""
    assert name == ""


def test_parse_table_name_with_schema_with_db(table_info: TableInfo):
    tn = "db.a.b"
    schema, name = table_info.parse_table_name(tn)
    # this looks wrong, but multiple databases are currently not supported
    assert schema == "db"
    assert name == "a.b"

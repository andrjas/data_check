import sys
import os
import pytest


# add the parent path to PYTHONPATH
my_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, my_path + "/../")

from data_check.sql import DataCheckSql  # noqa E402
from data_check.config import DataCheckConfig  # noqa E402


@pytest.fixture
def sql() -> DataCheckSql:
    dc_config = DataCheckConfig().load_config().set_connection("test")
    _sql = DataCheckSql(dc_config.connection)
    return _sql


def test_run_query(sql: DataCheckSql):
    if "oracle" in sql.connection:
        result = sql.run_query("select 'ok' as test_col from dual")
    else:
        result = sql.run_query("select 'ok' as test_col")
    assert result.iloc[0].test_col == "ok"


def test_test_connection(sql: DataCheckSql):
    test = sql.test_connection()
    assert test


def test_parse_table_name_with_schema(sql: DataCheckSql):
    tn = "a.b"
    schema, name = sql._parse_table_name(tn)
    assert schema == "a"
    assert name == "b"


def test_parse_table_name(sql: DataCheckSql):
    tn = "b"
    schema, name = sql._parse_table_name(tn)
    assert schema is None
    assert name == "b"


def test_parse_table_name_dot(sql: DataCheckSql):
    tn = "."
    schema, name = sql._parse_table_name(tn)
    assert schema == ""
    assert name == ""


def test_parse_table_name_with_schema_with_db(sql: DataCheckSql):
    tn = "db.a.b"
    schema, name = sql._parse_table_name(tn)
    # this looks wrong, but multiple databases are currently not supported
    assert schema == "db"
    assert name == "a.b"

import sys
import os
import pytest
from sqlalchemy.exc import OperationalError


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


def test_run_sql_query(sql: DataCheckSql):
    res = sql.run_sql("select 1 as test")
    assert res


def test_run_sql_query_fail(sql: DataCheckSql):
    with pytest.raises(OperationalError):
        sql.run_sql("select a from test")


def test_run_sql_ddl(sql: DataCheckSql):
    res = sql.run_sql("create table test (a varchar2)")
    assert res


def test_run_sql_ddl_and_query(sql: DataCheckSql):
    sql.run_sql("create table test (a varchar2)")
    res = sql.run_sql("select a from test")
    assert res == []


def test_run_sql_ddl_and_insert(sql: DataCheckSql):
    sql.run_sql("create table test (a varchar2)")
    sql.run_sql("insert into test values ('a')")
    res = sql.run_sql("select a from test")
    assert res == [("a",)]

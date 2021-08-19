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


def test_get_column_types_huge_date(sql: DataCheckSql):
    sql.run_sql("create table test_column_types (a varchar2, b decimal, c date)")
    sql.run_sql("insert into test_column_types values ('a', 1.13, date('9999-01-01'))")
    sq = sql.run_query_with_result("select a,b,c from test_column_types")
    assert sq.date_columns == ["c"]
    assert sq.string_columns == ["a"]


def test_get_column_types_varchar_date(sql: DataCheckSql):
    sql.run_sql("create table test_column_types (a varchar2, b decimal, c varchar2)")
    sql.run_sql("insert into test_column_types values ('a', 1.13, date('9999-01-01'))")
    sq = sql.run_query_with_result("select a,b,c from test_column_types")
    assert sq.date_columns == ["c"]
    assert sq.string_columns == ["a"]


def test_get_column_types_datetime(sql: DataCheckSql):
    sql.run_sql("create table test_column_types (a varchar2, b decimal, c varchar2)")
    sql.run_sql(
        "insert into test_column_types values ('a', 1.13, '9999-01-01 23:59:59.000')"
    )
    sq = sql.run_query_with_result("select a,b,c from test_column_types")
    assert sq.date_columns == ["c"]
    assert sq.string_columns == ["a"]


def test_get_column_types_mixed_dates(sql: DataCheckSql):
    sql.run_sql("create table test_column_types (a varchar2, b decimal, c varchar2)")
    sql.run_sql("insert into test_column_types values ('a', 1.13, '9999-01-01')")
    sql.run_sql("insert into test_column_types values ('a', 1.13, 'blub')")
    sq = sql.run_query_with_result("select a,b,c from test_column_types")
    assert sq.date_columns == []
    assert sq.string_columns == ["a", "c"]

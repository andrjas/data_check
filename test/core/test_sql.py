import pytest
from sqlalchemy.exc import OperationalError

from data_check.sql import DataCheckSql  # noqa E402


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
        sql.run_sql("select a from test_non_existing_table")


def test_run_sql_ddl(sql: DataCheckSql):
    res = sql.run_sql("create table test (a varchar2)")
    assert res


def test_run_sql_ddl_and_query(sql: DataCheckSql):
    sql.run_sql("create table test_run_sql_ddl_and_query (a varchar2)")
    res = sql.run_sql("select a from test_run_sql_ddl_and_query")
    assert res == []


def test_run_sql_ddl_and_insert(sql: DataCheckSql):
    sql.run_sql("create table test_run_sql_ddl_and_insert (a varchar2)")
    sql.run_sql("insert into test_run_sql_ddl_and_insert values ('a')")
    res = sql.run_sql("select a from test_run_sql_ddl_and_insert")
    assert res == [("a",)]


def test_get_column_types_huge_date(sql: DataCheckSql):
    sql.run_sql(
        "create table test_get_column_types_huge_date (a varchar2, b decimal, c date)"
    )
    sql.run_sql(
        "insert into test_get_column_types_huge_date values ('a', 1.13, date('9999-01-01'))"
    )
    sq = sql.run_query_with_result("select a,b,c from test_get_column_types_huge_date")
    assert sq.date_columns == ["c"]
    assert sq.string_columns == ["a"]


def test_get_column_types_varchar_date(sql: DataCheckSql):
    sql.run_sql(
        "create table test_get_column_types_varchar_date (a varchar2, b decimal, c varchar2)"
    )
    sql.run_sql(
        "insert into test_get_column_types_varchar_date values ('a', 1.13, date('9999-01-01'))"
    )
    sq = sql.run_query_with_result(
        "select a,b,c from test_get_column_types_varchar_date"
    )
    assert sq.date_columns == ["c"]
    assert sq.string_columns == ["a"]


def test_get_column_types_datetime(sql: DataCheckSql):
    sql.run_sql(
        "create table test_get_column_types_datetime (a varchar2, b decimal, c varchar2)"
    )
    sql.run_sql(
        "insert into test_get_column_types_datetime values ('a', 1.13, '9999-01-01 23:59:59.000')"
    )
    sq = sql.run_query_with_result("select a,b,c from test_get_column_types_datetime")
    assert sq.date_columns == ["c"]
    assert sq.string_columns == ["a"]


def test_get_column_types_mixed_dates(sql: DataCheckSql):
    sql.run_sql(
        "create table test_get_column_types_mixed_dates (a varchar2, b decimal, c varchar2)"
    )
    sql.run_sql(
        "insert into test_get_column_types_mixed_dates values ('a', 1.13, '9999-01-01')"
    )
    sql.run_sql(
        "insert into test_get_column_types_mixed_dates values ('a', 1.13, 'blub')"
    )
    sq = sql.run_query_with_result(
        "select a,b,c from test_get_column_types_mixed_dates"
    )
    assert sq.date_columns == []
    assert sq.string_columns == ["a", "c"]


def test_table_loaded_is_lazy_loaded(sql: DataCheckSql):
    tl1 = sql.table_loader
    tl2 = sql.table_loader
    assert tl1 == tl2

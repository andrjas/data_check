import sys
import os
import pytest
import pandas as pd
from pathlib import Path


# add the parent path to PYTHONPATH
my_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, my_path + "/../")

from data_check.sql import DataCheckSql, LoadMode  # noqa E402
from data_check.config import DataCheckConfig  # noqa E402


@pytest.fixture
def sql() -> DataCheckSql:
    dc_config = DataCheckConfig().load_config().set_connection("test")
    _sql = DataCheckSql(dc_config.connection)
    return _sql


def assert_equal_df(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
    df_diff = df1.merge(df2, how="outer", indicator=True)
    assert df_diff[df_diff["_merge"] != "both"].empty


def test_load_from_dataframe_append(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.get_connection().execute("create table test (id number(10), data varchar2(10))")
    sql.load_table("test", data, LoadMode.APPEND)
    df = sql.run_query("select id, data from test")
    assert_equal_df(data, df)


def test_load_from_dataframe_append_creates_table_if_no_table_exists(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.load_table("test", data, LoadMode.APPEND)
    df = sql.run_query("select id, data from test")
    assert_equal_df(data, df)


def test_load_from_dataframe_append_adds_data(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    data2 = pd.DataFrame.from_dict({"id": [3, 4, 5], "data": ["d", "e", "f"]})
    full_data = pd.DataFrame.from_dict(
        {"id": [0, 1, 2, 3, 4, 5], "data": ["a", "b", "c", "d", "e", "f"]}
    )
    sql.get_connection().execute("create table test (id number(10), data varchar2(10))")
    sql.load_table("test", data, LoadMode.APPEND)
    sql.load_table("test", data2, LoadMode.APPEND)
    df = sql.run_query("select id, data from test")
    assert_equal_df(full_data, df)


def test_load_from_dataframe_truncate(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.get_connection().execute("create table test (id number(10), data varchar2(10))")
    sql.load_table("test", data, LoadMode.TRUNCATE)
    df = sql.run_query("select id, data from test")
    assert_equal_df(data, df)


def test_load_from_dataframe_truncate_deletes_data(sql: DataCheckSql):
    sql.get_connection().execute("create table test (id number(10), data varchar2(10))")
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.load_table("test", data, LoadMode.TRUNCATE)
    data2 = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "d"]})
    sql.load_table("test", data2, LoadMode.TRUNCATE)
    df = sql.run_query("select id, data from test")
    assert_equal_df(data2, df)


def test_load_from_dataframe_truncate_creates_table_if_no_table_exists(
    sql: DataCheckSql,
):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.load_table("test", data, LoadMode.TRUNCATE)
    df = sql.run_query("select id, data from test")
    assert_equal_df(data, df)


def test_load_from_dataframe_replace(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.get_connection().execute("create table test (id number(10), data varchar2(10))")
    sql.load_table("test", data, LoadMode.REPLACE)
    df = sql.run_query("select id, data from test")
    assert_equal_df(data, df)


def test_load_from_dataframe_replace_creates_table_if_no_table_exists(
    sql: DataCheckSql,
):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.load_table("test", data, LoadMode.REPLACE)
    df = sql.run_query("select id, data from test")
    assert_equal_df(data, df)


def test_load_from_dataframe_replace_deletes_data(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.get_connection().execute("create table test (id number(10), data varchar2(10))")
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.load_table("test", data, LoadMode.REPLACE)
    data2 = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "d"]})
    sql.load_table("test", data2, LoadMode.REPLACE)
    df = sql.run_query("select id, data from test")
    assert_equal_df(data2, df)


def test_load_from_csv_file(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.get_connection().execute("create table test (id number(10), data varchar2(10))")
    sql.load_table_from_csv_file("test", Path("load_data/test.csv"), LoadMode.REPLACE)
    df = sql.run_query("select id, data from test")
    assert_equal_df(data, df)


def test_load_from_files(sql: DataCheckSql):
    data1 = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    data2 = pd.DataFrame.from_dict({"id": [3, 4, 5], "data": ["d", "e", "f"]})
    sql.load_tables_from_files([Path("load_data/tables")], LoadMode.REPLACE)
    df1 = sql.run_query("select id, data from test1")
    df2 = sql.run_query("select id, data from MAIN.TEST2")
    assert_equal_df(data1, df1)
    assert_equal_df(data2, df2)


def test_load_from_files_non_existing_dir(sql: DataCheckSql):
    with pytest.raises(Exception):
        sql.load_tables_from_files([Path("load_data/non_existing")], LoadMode.REPLACE)


def test_load_from_csv_file_load_modes(sql: DataCheckSql):
    sql.load_table_from_csv_file("test", Path("load_data/test.csv"), LoadMode.TRUNCATE)
    sql.load_table_from_csv_file("test", Path("load_data/test.csv"), LoadMode.APPEND)
    df = sql.run_query("select id, data from test")
    assert len(df) == 6


def test_load_from_csv_file_non_existing_file(sql: DataCheckSql):
    with pytest.raises(FileNotFoundError):
        sql.load_table_from_csv_file(
            "test", Path("load_data/non_existing.csv"), LoadMode.TRUNCATE
        )


def test_load_from_dataframe_schema(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.get_connection().execute(
        "create table MAIN.TEST (id number(10), data varchar2(10))"
    )
    sql.load_table("main.test", data, LoadMode.APPEND)
    df = sql.run_query("select id, data from MAIN.TEST")
    assert_equal_df(data, df)

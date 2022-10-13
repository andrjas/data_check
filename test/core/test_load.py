from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from data_check.sql import DataCheckSql, LoadMode  # noqa E402


@pytest.fixture(scope="module", params=["csv", "xlsx"])
def file_type(request):
    return request.param


def test_load_from_dataframe_append(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.get_connection().execute(
        "create table test_load_from_dataframe_append (id number(10), data varchar2(10))"
    )
    sql.table_loader.load_table(
        "test_load_from_dataframe_append", data, LoadMode.APPEND
    )
    df = sql.run_query("select id, data from test_load_from_dataframe_append")
    assert_frame_equal(data, df)


def test_load_from_dataframe_append_creates_table_if_no_table_exists(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.table_loader.load_table(
        "test_load_from_dataframe_append_creates_table_if_no_table_exists",
        data,
        LoadMode.APPEND,
    )
    df = sql.run_query(
        "select id, data from test_load_from_dataframe_append_creates_table_if_no_table_exists"
    )
    assert_frame_equal(data, df)


def test_load_from_dataframe_append_adds_data(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    data2 = pd.DataFrame.from_dict({"id": [3, 4, 5], "data": ["d", "e", "f"]})
    full_data = pd.DataFrame.from_dict(
        {"id": [0, 1, 2, 3, 4, 5], "data": ["a", "b", "c", "d", "e", "f"]}
    )
    sql.get_connection().execute(
        "create table test_load_from_dataframe_append_adds_data (id number(10), data varchar2(10))"
    )
    sql.table_loader.load_table(
        "test_load_from_dataframe_append_adds_data", data, LoadMode.APPEND
    )
    sql.table_loader.load_table(
        "test_load_from_dataframe_append_adds_data", data2, LoadMode.APPEND
    )
    df = sql.run_query("select id, data from test_load_from_dataframe_append_adds_data")
    assert_frame_equal(full_data, df)


def test_load_from_dataframe_truncate(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.get_connection().execute(
        "create table test_load_from_dataframe_truncate (id number(10), data varchar2(10))"
    )
    sql.table_loader.load_table(
        "test_load_from_dataframe_truncate", data, LoadMode.TRUNCATE
    )
    df = sql.run_query("select id, data from test_load_from_dataframe_truncate")
    assert_frame_equal(data, df)


def test_load_from_dataframe_truncate_deletes_data(sql: DataCheckSql):
    sql.get_connection().execute(
        "create table test_load_from_dataframe_truncate_deletes_data (id number(10), data varchar2(10))"
    )
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.table_loader.load_table(
        "test_load_from_dataframe_truncate_deletes_data", data, LoadMode.TRUNCATE
    )
    data2 = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "d"]})
    sql.table_loader.load_table(
        "test_load_from_dataframe_truncate_deletes_data", data2, LoadMode.TRUNCATE
    )
    df = sql.run_query(
        "select id, data from test_load_from_dataframe_truncate_deletes_data"
    )
    assert_frame_equal(data2, df)


def test_load_from_dataframe_truncate_creates_table_if_no_table_exists(
    sql: DataCheckSql,
):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.table_loader.load_table(
        "test_load_from_dataframe_truncate_creates_table_if_no_table_exists",
        data,
        LoadMode.TRUNCATE,
    )
    df = sql.run_query(
        "select id, data from test_load_from_dataframe_truncate_creates_table_if_no_table_exists"
    )
    assert_frame_equal(data, df)


def test_load_from_dataframe_replace(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.get_connection().execute(
        "create table test_load_from_dataframe_replace (id number(10), data varchar2(10))"
    )
    sql.table_loader.load_table(
        "test_load_from_dataframe_replace", data, LoadMode.REPLACE
    )
    df = sql.run_query("select id, data from test_load_from_dataframe_replace")
    assert_frame_equal(data, df)


def test_load_from_dataframe_replace_creates_table_if_no_table_exists(
    sql: DataCheckSql,
):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.table_loader.load_table(
        "test_load_from_dataframe_replace_creates_table_if_no_table_exists",
        data,
        LoadMode.REPLACE,
    )
    df = sql.run_query(
        "select id, data from test_load_from_dataframe_replace_creates_table_if_no_table_exists"
    )
    assert_frame_equal(data, df)


def test_load_from_dataframe_replace_deletes_data(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.get_connection().execute(
        "create table test_load_from_dataframe_replace_deletes_data (id number(10), data varchar2(10))"
    )
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.table_loader.load_table(
        "test_load_from_dataframe_replace_deletes_data", data, LoadMode.REPLACE
    )
    data2 = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "d"]})
    sql.table_loader.load_table(
        "test_load_from_dataframe_replace_deletes_data", data2, LoadMode.REPLACE
    )
    df = sql.run_query(
        "select id, data from test_load_from_dataframe_replace_deletes_data"
    )
    assert_frame_equal(data2, df)


def test_load_from_file(sql: DataCheckSql, file_type):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.get_connection().execute(
        f"create table test_load_from_file_{file_type} (id number(10), data varchar2(10))"
    )
    sql.table_loader.load_table_from_file(
        f"test_load_from_file_{file_type}",
        Path(f"load_data/test.{file_type}"),
        LoadMode.REPLACE,
    )
    df = sql.run_query(f"select id, data from test_load_from_file_{file_type}")
    assert_frame_equal(data, df)


def test_load_from_files(sql: DataCheckSql):
    data1 = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    data2 = pd.DataFrame.from_dict({"id": [3, 4, 5], "data": ["d", "e", "f"]})
    sql.table_loader.load_tables_from_files(
        [Path("load_data/tables")], LoadMode.REPLACE
    )
    df1 = sql.run_query("select id, data from test1")
    df2 = sql.run_query("select id, data from MAIN.TEST2")
    assert_frame_equal(data1, df1)
    assert_frame_equal(data2, df2)


def test_load_from_files_non_existing_dir(sql: DataCheckSql):
    with pytest.raises(Exception):
        sql.table_loader.load_tables_from_files(
            [Path("load_data/non_existing")], LoadMode.REPLACE
        )


def test_load_from_file_load_modes(sql: DataCheckSql, file_type):
    sql.table_loader.load_table_from_file(
        f"test_load_from_file_load_modes_{file_type}",
        Path(f"load_data/test.{file_type}"),
        LoadMode.TRUNCATE,
    )
    sql.table_loader.load_table_from_file(
        f"test_load_from_file_load_modes_{file_type}",
        Path(f"load_data/test.{file_type}"),
        LoadMode.APPEND,
    )
    df = sql.run_query(
        f"select id, data from test_load_from_file_load_modes_{file_type}"
    )
    assert len(df) == 6


def test_load_from_file_truncate_twice(sql: DataCheckSql, file_type):
    sql.table_loader.load_table_from_file(
        f"test_load_from_file_truncate_twice_{file_type}",
        Path(f"load_data/test.{file_type}"),
        LoadMode.TRUNCATE,
    )
    sql.table_loader.load_table_from_file(
        f"test_load_from_file_truncate_twice_{file_type}",
        Path(f"load_data/test.{file_type}"),
        LoadMode.TRUNCATE,
    )
    df = sql.run_query(
        f"select id, data from test_load_from_file_truncate_twice_{file_type}"
    )
    assert len(df) == 3


def test_load_from_file_non_existing_file(sql: DataCheckSql, file_type):
    with pytest.raises(FileNotFoundError):
        sql.table_loader.load_table_from_file(
            f"test_{file_type}",
            Path(f"load_data/non_existing.{file_type}"),
            LoadMode.TRUNCATE,
        )


def test_load_from_dataframe_schema(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.get_connection().execute(
        "create table main.test_load_from_dataframe_schema (id number(10), data varchar2(10))"
    )
    sql.table_loader.load_table(
        "main.test_load_from_dataframe_schema", data, LoadMode.APPEND
    )
    df = sql.run_query("select id, data from main.test_load_from_dataframe_schema")
    assert_frame_equal(data, df)


def test_load_leading_zeros_string(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["123", "012", "000"]})
    sql.get_connection().execute(
        "create table temp.test_load_leading_zeros_string (id number(10), data varchar2(10))"
    )
    sql.table_loader.load_table(
        "temp.test_load_leading_zeros_string", data, LoadMode.TRUNCATE
    )
    df = sql.run_query("select id, data from temp.test_load_leading_zeros_string")
    assert_frame_equal(data, df)


def test_load_table_from_file_load_mode_is_deprecated(sql: DataCheckSql):
    with pytest.warns(FutureWarning):
        sql.table_loader.load_table_from_file(
            "test_load_table_from_file_load_mode_is_deprecated",
            Path(f"load_data/test.csv"),
            load_mode=LoadMode.TRUNCATE,
        )


def test_load_table_from_file_load_mode_is_deprecated_as_str(sql: DataCheckSql):
    with pytest.warns(FutureWarning):
        sql.table_loader.load_table_from_file(
            "test_load_table_from_file_load_mode_is_deprecated_as_str",
            Path(f"load_data/test.csv"),
            load_mode="truncate",
        )


def test_load_tables_from_files_load_mode_is_deprecated(sql: DataCheckSql):
    with pytest.warns(FutureWarning):
        sql.table_loader.load_tables_from_files(
            [Path(f"load_data/test.csv")], load_mode=LoadMode.TRUNCATE
        )


def test_load_tables_from_files_load_mode_is_deprecated_as_str(sql: DataCheckSql):
    with pytest.warns(FutureWarning):
        sql.table_loader.load_tables_from_files(
            [Path(f"load_data/test.csv")], load_mode="truncate"
        )

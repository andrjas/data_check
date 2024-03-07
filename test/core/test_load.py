from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from sqlalchemy import text

from data_check.exceptions import DataCheckError
from data_check.sql import DataCheckSql, LoadMode
from data_check.sql.table import Table


@pytest.fixture(scope="module", params=["csv", "xlsx"])
def file_type(request):
    return request.param


def test_load_from_dataframe_append(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    table = Table(sql, "test_load_from_dataframe_append")
    with sql.conn() as c:
        c.execute(text(f"create table {table} (id number(10), data varchar2(10))"))
    sql.table_loader.load_table(table, data, LoadMode.APPEND)
    df = sql.run_query(f"select id, data from {table}")
    assert_frame_equal(data, df)


def test_load_from_dataframe_append_creates_table_if_no_table_exists(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    table = Table(
        sql, "test_load_from_dataframe_append_creates_table_if_no_table_exists"
    )
    sql.table_loader.load_table(
        table,
        data,
        LoadMode.APPEND,
    )
    df = sql.run_query(f"select id, data from {table}")
    assert_frame_equal(data, df)


def test_load_from_dataframe_append_adds_data(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    data2 = pd.DataFrame.from_dict({"id": [3, 4, 5], "data": ["d", "e", "f"]})
    full_data = pd.DataFrame.from_dict(
        {"id": [0, 1, 2, 3, 4, 5], "data": ["a", "b", "c", "d", "e", "f"]}
    )
    table = Table(sql, "test_load_from_dataframe_append_adds_data")
    with sql.conn() as c:
        c.execute(text(f"create table {table} (id number(10), data varchar2(10))"))
    sql.table_loader.load_table(table, data, LoadMode.APPEND)
    sql.table_loader.load_table(table, data2, LoadMode.APPEND)
    df = sql.run_query(f"select id, data from {table}")
    assert_frame_equal(full_data, df)


def test_load_from_dataframe_truncate(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    table = Table(sql, "test_load_from_dataframe_truncate")
    with sql.conn() as c:
        c.execute(text(f"create table {table} (id number(10), data varchar2(10))"))
    sql.table_loader.load_table(table, data, LoadMode.TRUNCATE)
    df = sql.run_query(f"select id, data from {table}")
    assert_frame_equal(data, df)


def test_load_from_dataframe_truncate_deletes_data(sql: DataCheckSql):
    table = Table(sql, "test_load_from_dataframe_truncate_deletes_data")
    with sql.conn() as c:
        c.execute(text(f"create table {table} (id number(10), data varchar2(10))"))
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.table_loader.load_table(table, data, LoadMode.TRUNCATE)
    data2 = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "d"]})
    sql.table_loader.load_table(table, data2, LoadMode.TRUNCATE)
    df = sql.run_query(f"select id, data from {table}")
    assert_frame_equal(data2, df)


def test_load_from_dataframe_truncate_creates_table_if_no_table_exists(
    sql: DataCheckSql,
):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    table = Table(
        sql, "test_load_from_dataframe_truncate_creates_table_if_no_table_exists"
    )
    sql.table_loader.load_table(
        table,
        data,
        LoadMode.TRUNCATE,
    )
    df = sql.run_query(f"select id, data from {table}")
    assert_frame_equal(data, df)


def test_load_from_dataframe_replace(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    table = Table(sql, "test_load_from_dataframe_replace")
    with sql.conn() as c:
        c.execute(text(f"create table {table} (id number(10), data varchar2(10))"))
    sql.table_loader.load_table(table, data, LoadMode.REPLACE)
    df = sql.run_query(f"select id, data from {table}")
    assert_frame_equal(data, df)


def test_load_from_dataframe_replace_creates_table_if_no_table_exists(
    sql: DataCheckSql,
):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    table = Table(
        sql, "test_load_from_dataframe_replace_creates_table_if_no_table_exists"
    )
    sql.table_loader.load_table(
        table,
        data,
        LoadMode.REPLACE,
    )
    df = sql.run_query(f"select id, data from {table}")
    assert_frame_equal(data, df)


def test_load_from_dataframe_replace_deletes_data(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    table = Table(sql, "test_load_from_dataframe_replace_deletes_data")
    with sql.conn() as c:
        c.execute(text(f"create table {table} (id number(10), data varchar2(10))"))
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.table_loader.load_table(table, data, LoadMode.REPLACE)
    data2 = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "d"]})
    sql.table_loader.load_table(table, data2, LoadMode.REPLACE)
    df = sql.run_query(f"select id, data from {table}")
    assert_frame_equal(data2, df)


def test_load_from_file(sql: DataCheckSql, file_type):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    with sql.conn() as c:
        c.execute(
            text(
                f"create table test_load_from_file_{file_type} "
                "(id number(10), data varchar2(10))"
            )
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
    with pytest.raises(FileNotFoundError):
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
    row_count = 6
    assert len(df) == row_count


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
    row_count = 3
    assert len(df) == row_count


def test_load_from_file_non_existing_file(sql: DataCheckSql, file_type):
    with pytest.raises((DataCheckError, FileNotFoundError)):
        sql.table_loader.load_table_from_file(
            f"test_{file_type}",
            Path(f"load_data/non_existing.{file_type}"),
            LoadMode.TRUNCATE,
        )


def test_load_from_dataframe_schema(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    table = Table.from_table_name(sql, "temp.test_load_from_dataframe_schema")
    with sql.conn() as c:
        c.execute(text(f"create table {table} (id number(10), data varchar2(10))"))
    # need to disconnect here, as otherwise pandas inspector request is cached
    # and returns that the table has not been created
    sql.disconnect()
    sql.table_loader.load_table(table, data, LoadMode.APPEND)
    df = sql.run_query(f"select id, data from {table}")
    assert_frame_equal(data, df)


def test_load_leading_zeros_string(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["123", "012", "000"]})
    table = Table.from_table_name(sql, "temp.test_load_leading_zeros_string")
    with sql.conn() as c:
        c.execute(text(f"create table {table} (id number(10), data varchar2(10))"))
    sql.disconnect()  # see test_load_from_dataframe_schema

    sql.table_loader.load_table(table, data, LoadMode.TRUNCATE)
    df = sql.run_query(f"select id, data from {table}")
    assert_frame_equal(data, df)


def test_load_table_from_file_load_mode_is_deprecated(sql: DataCheckSql):
    with pytest.warns(FutureWarning):
        sql.table_loader.load_table_from_file(
            "test_load_table_from_file_load_mode_is_deprecated",
            Path("load_data/test.csv"),
            load_mode=LoadMode.TRUNCATE,
        )


def test_load_table_from_file_load_mode_is_deprecated_as_str(sql: DataCheckSql):
    with pytest.warns(FutureWarning):
        sql.table_loader.load_table_from_file(
            "test_load_table_from_file_load_mode_is_deprecated_as_str",
            Path("load_data/test.csv"),
            load_mode="truncate",
        )


def test_load_tables_from_files_load_mode_is_deprecated(sql: DataCheckSql):
    with pytest.warns(FutureWarning):
        sql.table_loader.load_tables_from_files(
            [Path("load_data/test.csv")], load_mode=LoadMode.TRUNCATE
        )


def test_load_tables_from_files_load_mode_is_deprecated_as_str(sql: DataCheckSql):
    with pytest.warns(FutureWarning):
        sql.table_loader.load_tables_from_files(
            [Path("load_data/test.csv")], load_mode="truncate"
        )


def test_default_load_mode_is_truncate(sql: DataCheckSql):
    table_loader = sql.table_loader
    assert table_loader.default_load_mode == LoadMode.TRUNCATE


def test_default_load_mode_set_append(sql: DataCheckSql):
    sql.config.config["default_load_mode"] = "append"
    table_loader = sql.table_loader
    assert table_loader.default_load_mode == LoadMode.APPEND


def test_get_load_mode_default(sql: DataCheckSql):
    mode = sql.table_loader.get_load_mode(None, LoadMode.DEFAULT)
    assert mode == LoadMode.TRUNCATE


def test_get_load_mode_set_append(sql: DataCheckSql):
    sql.config.config["default_load_mode"] = "append"
    mode = sql.table_loader.get_load_mode(None, LoadMode.DEFAULT)
    assert mode == LoadMode.APPEND

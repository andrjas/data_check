import sys
import os
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
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


def test_load_from_dataframe_append(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.get_connection().execute("create table test (id number(10), data varchar2(10))")
    sql.table_loader.load_table("test", data, LoadMode.APPEND)
    df = sql.run_query("select id, data from test")
    assert_frame_equal(data, df)


def test_load_from_dataframe_append_creates_table_if_no_table_exists(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.table_loader.load_table("test", data, LoadMode.APPEND)
    df = sql.run_query("select id, data from test")
    assert_frame_equal(data, df)


def test_load_from_dataframe_append_adds_data(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    data2 = pd.DataFrame.from_dict({"id": [3, 4, 5], "data": ["d", "e", "f"]})
    full_data = pd.DataFrame.from_dict(
        {"id": [0, 1, 2, 3, 4, 5], "data": ["a", "b", "c", "d", "e", "f"]}
    )
    sql.get_connection().execute("create table test (id number(10), data varchar2(10))")
    sql.table_loader.load_table("test", data, LoadMode.APPEND)
    sql.table_loader.load_table("test", data2, LoadMode.APPEND)
    df = sql.run_query("select id, data from test")
    assert_frame_equal(full_data, df)


def test_load_from_dataframe_truncate(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.get_connection().execute("create table test (id number(10), data varchar2(10))")
    sql.table_loader.load_table("test", data, LoadMode.TRUNCATE)
    df = sql.run_query("select id, data from test")
    assert_frame_equal(data, df)


def test_load_from_dataframe_truncate_deletes_data(sql: DataCheckSql):
    sql.get_connection().execute("create table test (id number(10), data varchar2(10))")
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.table_loader.load_table("test", data, LoadMode.TRUNCATE)
    data2 = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "d"]})
    sql.table_loader.load_table("test", data2, LoadMode.TRUNCATE)
    df = sql.run_query("select id, data from test")
    assert_frame_equal(data2, df)


def test_load_from_dataframe_truncate_creates_table_if_no_table_exists(
    sql: DataCheckSql,
):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.table_loader.load_table("test", data, LoadMode.TRUNCATE)
    df = sql.run_query("select id, data from test")
    assert_frame_equal(data, df)


def test_load_from_dataframe_replace(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.get_connection().execute("create table test (id number(10), data varchar2(10))")
    sql.table_loader.load_table("test", data, LoadMode.REPLACE)
    df = sql.run_query("select id, data from test")
    assert_frame_equal(data, df)


def test_load_from_dataframe_replace_creates_table_if_no_table_exists(
    sql: DataCheckSql,
):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.table_loader.load_table("test", data, LoadMode.REPLACE)
    df = sql.run_query("select id, data from test")
    assert_frame_equal(data, df)


def test_load_from_dataframe_replace_deletes_data(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.get_connection().execute("create table test (id number(10), data varchar2(10))")
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.table_loader.load_table("test", data, LoadMode.REPLACE)
    data2 = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "d"]})
    sql.table_loader.load_table("test", data2, LoadMode.REPLACE)
    df = sql.run_query("select id, data from test")
    assert_frame_equal(data2, df)


def test_load_from_csv_file(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.get_connection().execute("create table test (id number(10), data varchar2(10))")
    sql.table_loader.load_table_from_csv_file(
        "test", Path("load_data/test.csv"), LoadMode.REPLACE
    )
    df = sql.run_query("select id, data from test")
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


def test_load_from_csv_file_load_modes(sql: DataCheckSql):
    sql.table_loader.load_table_from_csv_file(
        "test", Path("load_data/test.csv"), LoadMode.TRUNCATE
    )
    sql.table_loader.load_table_from_csv_file(
        "test", Path("load_data/test.csv"), LoadMode.APPEND
    )
    df = sql.run_query("select id, data from test")
    assert len(df) == 6


def test_load_from_csv_file_non_existing_file(sql: DataCheckSql):
    with pytest.raises(FileNotFoundError):
        sql.table_loader.load_table_from_csv_file(
            "test", Path("load_data/non_existing.csv"), LoadMode.TRUNCATE
        )


def test_load_from_dataframe_schema(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    sql.get_connection().execute(
        "create table MAIN.TEST (id number(10), data varchar2(10))"
    )
    sql.table_loader.load_table("main.test", data, LoadMode.APPEND)
    df = sql.run_query("select id, data from MAIN.TEST")
    assert_frame_equal(data, df)


def test_parse_table_name_with_schema(sql: DataCheckSql):
    tn = "a.b"
    schema, name = sql.table_loader._parse_table_name(tn)
    assert schema == "a"
    assert name == "b"


def test_parse_table_name(sql: DataCheckSql):
    tn = "b"
    schema, name = sql.table_loader._parse_table_name(tn)
    assert schema is None
    assert name == "b"


def test_parse_table_name_dot(sql: DataCheckSql):
    tn = "."
    schema, name = sql.table_loader._parse_table_name(tn)
    assert schema == ""
    assert name == ""


def test_parse_table_name_with_schema_with_db(sql: DataCheckSql):
    tn = "db.a.b"
    schema, name = sql.table_loader._parse_table_name(tn)
    # this looks wrong, but multiple databases are currently not supported
    assert schema == "db"
    assert name == "a.b"


def test_load_leading_zeros_string(sql: DataCheckSql):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["123", "012", "000"]})
    sql.get_connection().execute(
        "create table TEMP.TEST (id number(10), data varchar2(10))"
    )
    sql.table_loader.load_table("temp.test", data, LoadMode.TRUNCATE)
    df = sql.run_query("select id, data from TEMP.TEST")
    assert_frame_equal(data, df)

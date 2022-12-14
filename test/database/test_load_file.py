import datetime
from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from sqlalchemy import Column, Date, DateTime, Integer, MetaData, Numeric, String
from sqlalchemy import Table as SQLTable

from data_check import DataCheck  # noqa E402
from data_check.sql import LoadMode, Table  # noqa E402

# These tests should work on any database.
# The tests are generic, but in integration tests each database uses specific SQL files.


@pytest.fixture(scope="module", params=["csv", "xlsx"])
def file_type(request) -> str:
    return request.param


def create_test_table(table_name: str, schema: str, dc: DataCheck):
    table = Table(dc.sql, table_name, schema)
    table.drop_if_exists()
    if dc.sql.dialect == "oracle":
        dc.sql.run_sql(f"create table {table} (id decimal, data varchar2(10))")
    else:
        metadata = MetaData(dc.sql.get_engine())
        SQLTable(
            table.name,
            metadata,
            Column("id", Integer),
            Column("data", String(10)),
            schema=table.schema,
        )
        metadata.create_all()
    return table


def create_test_table_with_date(table_name: str, schema: str, dc: DataCheck):
    table = Table(dc.sql, table_name, schema)
    table.drop_if_exists()
    if dc.sql.dialect == "oracle":
        dc.sql.run_sql(
            (f"create table {table} (id number(10), data varchar2(10), dat date)")
        )
    else:
        metadata = MetaData(dc.sql.get_engine())
        SQLTable(
            table.name,
            metadata,
            Column("id", Integer),
            Column("data", String(10)),
            Column("dat", Date),
            schema=table.schema,
        )
        metadata.create_all()
    return table


def create_test_table_with_datetime(table_name: str, schema: str, dc: DataCheck):
    table = Table(dc.sql, table_name, schema)
    table.drop_if_exists()
    if dc.sql.dialect == "oracle":
        dc.sql.run_sql(
            (f"create table {table} " "(id number(10), data varchar2(10), dat date)")
        )
    else:
        metadata = MetaData(dc.sql.get_engine())
        SQLTable(
            table.name,
            metadata,
            Column("id", Integer),
            Column("data", String(10)),
            Column("dat", DateTime),
            schema=table.schema,
        )
        metadata.create_all()
    return table


def create_test_table_with_decimal(table_name: str, schema: str, dc: DataCheck):
    table = Table(dc.sql, table_name, schema)
    table.drop_if_exists()
    if dc.sql.dialect == "oracle":
        dc.sql.run_sql(
            (
                f"create table {table} (id number(10), data varchar2(10), decim decimal(10, 4))"
            )
        )
    else:
        metadata = MetaData(dc.sql.get_engine())
        SQLTable(
            table.name,
            metadata,
            Column("id", Integer),
            Column("data", String(10)),
            Column("decim", Numeric(10, 4)),
            schema=table.schema,
        )
        metadata.create_all()
    return table


def create_test_table_with_large_decimal(table_name: str, schema: str, dc: DataCheck):
    table = Table(dc.sql, table_name, schema)
    table.drop_if_exists()
    if dc.sql.dialect == "oracle":
        dc.sql.run_sql((f"create table {table} (d_col decimal(38, 0))"))
    else:
        metadata = MetaData(dc.sql.get_engine())
        SQLTable(
            table.name,
            metadata,
            Column("d_col", Numeric(38, 0)),
            schema=table.schema,
        )
        metadata.create_all()
    return table


def create_test_table_sample(table_name: str, schema: str, dc: DataCheck):
    table = Table(dc.sql, table_name, schema)
    table.drop_if_exists()
    if dc.sql.dialect == "oracle":
        dc.sql.run_sql(
            (
                f"create table {table} "
                "(a number(10), b number(10), c varchar2(10), d number(10), e varchar2(10), f number(10), g number(10), h date, i date, j date, k varchar2(10), l number(10), m date)"
            )
        )
    else:
        metadata = MetaData(dc.sql.get_engine())
        SQLTable(
            table.name,
            metadata,
            Column("a", Integer),
            Column("b", Integer),
            Column("c", String(10)),
            Column("d", Integer),
            Column("e", String(10)),
            Column("f", Integer),
            Column("g", Integer),
            Column("h", DateTime),
            Column("i", DateTime),
            Column("j", DateTime),
            Column("k", String(10)),
            Column("l", Integer),
            Column("m", DateTime),
            schema=table.schema,
        )
        metadata.create_all()
    return table


def test_load_file_replace(dc_serial: DataCheck, file_type: str):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    dc_serial.sql.table_loader.load_table_from_file(
        f"main.test_replace_{file_type}",
        Path(f"load_data/test.{file_type}"),
        LoadMode.REPLACE,
    )
    df = dc_serial.sql.run_query(f"select id, data from main.test_replace_{file_type}")
    assert_frame_equal(data, df)


def test_load_file_replace_with_table(dc_serial: DataCheck, file_type: str):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    create_test_table(f"test_replace2_{file_type}", "main", dc_serial)
    dc_serial.sql.table_loader.load_table_from_file(
        f"main.test_replace2_{file_type}",
        Path(f"load_data/test.{file_type}"),
        LoadMode.REPLACE,
    )
    df = dc_serial.sql.run_query(f"select id, data from main.test_replace2_{file_type}")
    assert_frame_equal(data, df)


def test_load_file_truncate(dc_serial: DataCheck, file_type: str):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    dc_serial.sql.table_loader.load_table_from_file(
        f"main.test_truncate_{file_type}",
        Path(f"load_data/test.{file_type}"),
        LoadMode.TRUNCATE,
    )
    df = dc_serial.sql.run_query(f"select id, data from main.test_truncate_{file_type}")
    assert_frame_equal(data, df)


def test_load_file_truncate_with_table(dc_serial: DataCheck, file_type: str):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    create_test_table(f"test_truncate2_{file_type}", "main", dc_serial)
    dc_serial.sql.table_loader.load_table_from_file(
        f"main.test_truncate2_{file_type}",
        Path(f"load_data/test.{file_type}"),
        LoadMode.TRUNCATE,
    )
    df = dc_serial.sql.run_query(
        f"select id, data from main.test_truncate2_{file_type}"
    )
    assert_frame_equal(data, df)


def test_load_file_append(dc_serial: DataCheck, file_type: str):
    dc_serial.sql.table_loader.load_table_from_file(
        f"main.test_append_{file_type}",
        Path(f"load_data/test.{file_type}"),
        LoadMode.TRUNCATE,
    )
    dc_serial.sql.table_loader.load_table_from_file(
        f"main.test_append_{file_type}",
        Path(f"load_data/test.{file_type}"),
        LoadMode.APPEND,
    )
    df = dc_serial.sql.run_query(f"select id, data from main.test_append_{file_type}")
    check_data = pd.DataFrame.from_dict(
        {"id": [0, 1, 2, 0, 1, 2], "data": ["a", "b", "c", "a", "b", "c"]}
    )
    assert_frame_equal(check_data, df)
    assert len(df) == 6


def test_load_file_append_with_table(dc_serial: DataCheck, file_type: str):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    create_test_table(f"test_append2_{file_type}", "main", dc_serial)
    dc_serial.sql.table_loader.load_table_from_file(
        f"main.test_append2_{file_type}",
        Path(f"load_data/test.{file_type}"),
        LoadMode.APPEND,
    )
    df = dc_serial.sql.run_query(f"select id, data from main.test_append2_{file_type}")
    assert_frame_equal(data, df)
    assert len(df) == 3


def test_load_file_date_type(dc_serial: DataCheck, file_type: str):
    create_test_table_with_date(f"test_date_{file_type}", "main", dc_serial)
    dc_serial.sql.table_loader.load_table_from_file(
        f"main.test_date_{file_type}",
        Path(f"load_data/test_date.{file_type}"),
        LoadMode.TRUNCATE,
    )
    df = dc_serial.sql.run_query(
        f"select id, data, dat from main.test_date_{file_type}"
    )
    dat = df.dat
    assert not dat.empty


def test_load_file_date_type_huge_date(dc_serial: DataCheck, file_type: str):
    data = pd.DataFrame.from_dict(
        {
            "id": [0, 1],
            "data": ["a", "b"],
            "dat": [datetime.datetime(2021, 1, 25), datetime.datetime(9999, 12, 31)],
        }
    )
    create_test_table_with_datetime(f"test_date_huge_{file_type}", "main", dc_serial)
    dc_serial.sql.table_loader.load_table_from_file(
        f"main.test_date_huge_{file_type}",
        Path(f"load_data/test_date_huge.{file_type}"),
        LoadMode.TRUNCATE,
    )
    df = dc_serial.sql.run_query(
        f"select id, data, dat from main.test_date_huge_{file_type}"
    )
    assert_frame_equal(data, df)


def test_load_file_datetime_type(dc_serial: DataCheck, file_type: str):
    create_test_table_with_datetime(f"test_datetime_{file_type}", "main", dc_serial)
    dc_serial.sql.table_loader.load_table_from_file(
        f"main.test_datetime_{file_type}",
        Path(f"load_data/test_datetime.{file_type}"),
        LoadMode.TRUNCATE,
    )
    df = dc_serial.sql.run_query(
        f"select id, data, dat from main.test_datetime_{file_type}"
    )
    dat = df.dat
    assert not dat.empty


def test_load_file_date_with_existing_table_replace(
    dc_serial: DataCheck, file_type: str
):
    create_test_table_with_datetime(f"test_date_replace_{file_type}", "main", dc_serial)
    dc_serial.sql.table_loader.load_table_from_file(
        f"main.test_date_replace_{file_type}",
        Path(f"load_data/test_date.{file_type}"),
        LoadMode.REPLACE,
    )
    df = dc_serial.sql.run_query(
        f"select id, data, dat from main.test_date_replace_{file_type}"
    )
    if dc_serial.sql.dialect == "oracle":
        # in Oracle this is a date type
        assert df.dat.dtype == "<M8[ns]"
    else:
        assert df.dat.dtype == "datetime64[ns]"


def test_load_file_decimal_type(dc_serial: DataCheck, file_type: str):
    data = pd.DataFrame.from_dict(
        {"id": [0, 1], "data": ["a", "b"], "decim": [0.1, 0.2]}
    )
    create_test_table_with_decimal(f"test_decimals_{file_type}", "main", dc_serial)
    dc_serial.sql.table_loader.load_table_from_file(
        f"main.test_decimals_{file_type}",
        Path(f"load_data/test_decimals.{file_type}"),
        LoadMode.TRUNCATE,
    )
    df = dc_serial.sql.run_query(
        f"select id, data, decim from main.test_decimals_{file_type}"
    )
    assert_frame_equal(data, df)


def test_load_file_less_columns_in_file(dc_serial: DataCheck, file_type: str):
    data = pd.DataFrame.from_dict(
        {"id": [0, 1, 2], "data": ["a", "b", "c"], "decim": [pd.NA, pd.NA, pd.NA]}
    )
    create_test_table_with_decimal(
        f"test_less_columns_in_{file_type}", "main", dc_serial
    )
    dc_serial.sql.table_loader.load_table_from_file(
        f"main.test_less_columns_in_{file_type}",
        Path(f"load_data/test.{file_type}"),
        LoadMode.TRUNCATE,
    )
    df = dc_serial.sql.run_query(
        f"select id, data, decim from main.test_less_columns_in_{file_type}"
    )
    assert_frame_equal(data, df)


def test_load_file_more_columns_in_file(dc_serial: DataCheck, file_type: str):
    create_test_table(f"test_more_columns_in_{file_type}", "main", dc_serial)
    with pytest.raises(Exception):
        dc_serial.sql.table_loader.load_table_from_file(
            f"main.test_more_columns_in_{file_type}",
            Path(f"load_data/test_decimals.{file_type}"),
            LoadMode.TRUNCATE,
        )


def test_table_exists(dc_serial: DataCheck):
    table = create_test_table("test_table_exists", "main", dc_serial)
    assert table.exists()


def test_table_exists_non_existing(dc_serial: DataCheck):
    table = Table(dc_serial.sql, "test_table_exists_non_existing", "main")
    assert not table.exists()


def test_drop_table_if_exists_with_existing_table(dc_serial: DataCheck):
    table = create_test_table("test_drop_existing", "main", dc_serial)
    table.drop_if_exists()
    with pytest.raises(Exception):
        dc_serial.sql.run_query("select * from main.test_drop_existing")


def test_drop_table_if_exists_with_non_existing_table(dc_serial: DataCheck):
    table = Table(dc_serial.sql, "test_drop_non_existing", "main")
    table.drop_if_exists()
    with pytest.raises(Exception):
        dc_serial.sql.run_query("select * from main.test_drop_non_existing")


def test_load_file_with_null_dates(dc_serial: DataCheck):
    table = Table(dc_serial.sql, "test_with_null_dates", "main")
    table.drop_if_exists()
    dc_serial.sql.table_loader.load_table_from_file(
        "main.test_with_null_dates",
        Path("load_data/sample/test_date_with_null_dates.csv"),
        LoadMode.TRUNCATE,
    )
    assert table.exists()


def test_load_file_with_null_dates_with_existing_table(dc_serial: DataCheck):
    table = create_test_table_sample(
        "test_with_null_dates_existing_table", "main", dc_serial
    )
    dc_serial.sql.table_loader.load_table_from_file(
        "main.test_with_null_dates_existing_table",
        Path("load_data/sample/test_date_with_null_dates.csv"),
        LoadMode.TRUNCATE,
    )
    assert table.exists()
    data = dc_serial.sql.run_query(
        "select * from main.test_with_null_dates_existing_table"
    )
    assert not data.empty


def test_load_large_number(dc_serial: DataCheck):
    create_test_table_with_large_decimal("test_load_large_number", "main", dc_serial)
    csv = Path("load_data/sample/large_number_test.csv")
    dc_serial.sql.table_loader.load_table_from_file(
        "main.test_load_large_number",
        csv,
        LoadMode.TRUNCATE,
    )

    check = dc_serial.collect_checks([csv])
    assert check
    table_check = check[0]
    table_check.check_path = Path("main.test_load_large_number.csv")
    result = table_check.run_test()

    assert result

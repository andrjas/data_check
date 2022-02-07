from pathlib import Path
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
from sqlalchemy import Table, Column, String, Integer, MetaData, Date, Numeric, DateTime
import datetime


from data_check import DataCheck  # noqa E402
from data_check.config import DataCheckConfig  # noqa E402
from data_check.sql import LoadMode  # noqa E402

# These tests should work on any database.
# The tests are generic, but in integration tests each database uses specific SQL files.


@pytest.fixture(scope="module", params=["csv", "xlsx"])
def file_type(request) -> str:
    return request.param


@pytest.fixture
def dc() -> DataCheck:
    config = DataCheckConfig().load_config().set_connection("test")
    config.parallel_workers = 1
    _dc = DataCheck(config)
    _dc.load_template()
    _dc.output.configure_output(
        verbose=True,
        traceback=True,
        print_failed=True,
        print_format="json",
    )
    return _dc


def create_test_table(table_name: str, schema: str, dc: DataCheck):
    if dc.sql.dialect == "oracle":
        dc.sql.run_sql(
            f"create table {schema}.{table_name} (id number(10), data varchar2(10))"
        )
    elif dc.sql.dialect == "sqlite":
        dc.sql.run_sql(
            f"create table {schema}.{table_name} (id decimal, data varchar(10))"
        )
    else:
        metadata = MetaData(dc.sql.get_engine())
        Table(
            table_name,
            metadata,
            Column("id", Integer),
            Column("data", String(10)),
            schema=schema,
        )
        metadata.create_all()


def create_test_table_with_date(table_name: str, schema: str, dc: DataCheck):
    if dc.sql.dialect == "oracle":
        dc.sql.run_sql(
            (
                f"create table {schema}.{table_name} "
                "(id number(10), data varchar2(10), dat date)"
            )
        )
    else:
        metadata = MetaData(dc.sql.get_engine())
        Table(
            table_name,
            metadata,
            Column("id", Integer),
            Column("data", String(10)),
            Column("dat", Date),
            schema=schema,
        )
        metadata.create_all()


def create_test_table_with_datetime(table_name: str, schema: str, dc: DataCheck):
    if dc.sql.dialect == "oracle":
        dc.sql.run_sql(
            (
                f"create table {schema}.{table_name} "
                "(id number(10), data varchar2(10), dat date)"
            )
        )
    else:
        metadata = MetaData(dc.sql.get_engine())
        Table(
            table_name,
            metadata,
            Column("id", Integer),
            Column("data", String(10)),
            Column("dat", DateTime),
            schema=schema,
        )
        metadata.create_all()


def create_test_table_with_decimal(table_name: str, schema: str, dc: DataCheck):
    if dc.sql.dialect == "oracle":
        dc.sql.run_sql(
            (
                f"create table {schema}.{table_name} "
                "(id number(10), data varchar2(10), decim decimal(10, 4))"
            )
        )
    elif dc.sql.dialect == "sqlite":
        dc.sql.run_sql(
            (
                f"create table {schema}.{table_name} "
                "(id decimal, data varchar(10), decim decimal)"
            )
        )
    else:
        metadata = MetaData(dc.sql.get_engine())
        Table(
            table_name,
            metadata,
            Column("id", Integer),
            Column("data", String(10)),
            Column("decim", Numeric(10, 4)),
            schema=schema,
        )
        metadata.create_all()


def test_load_file_replace(dc: DataCheck, file_type: str):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    dc.sql.table_loader.load_table_from_file(
        f"main.test_replace_{file_type}",
        Path(f"load_data/test.{file_type}"),
        LoadMode.REPLACE,
    )
    df = dc.sql.run_query(f"select id, data from main.test_replace_{file_type}")
    assert_frame_equal(data, df)


def test_load_file_replace_with_table(dc: DataCheck, file_type: str):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    create_test_table(f"test_replace2_{file_type}", "main", dc)
    dc.sql.table_loader.load_table_from_file(
        f"main.test_replace2_{file_type}",
        Path(f"load_data/test.{file_type}"),
        LoadMode.REPLACE,
    )
    df = dc.sql.run_query(f"select id, data from main.test_replace2_{file_type}")
    assert_frame_equal(data, df)


def test_load_file_truncate(dc: DataCheck, file_type: str):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    dc.sql.table_loader.load_table_from_file(
        f"main.test_truncate_{file_type}",
        Path(f"load_data/test.{file_type}"),
        LoadMode.TRUNCATE,
    )
    df = dc.sql.run_query(f"select id, data from main.test_truncate_{file_type}")
    assert_frame_equal(data, df)


def test_load_file_truncate_with_table(dc: DataCheck, file_type: str):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    create_test_table(f"test_truncate2_{file_type}", "main", dc)
    dc.sql.table_loader.load_table_from_file(
        f"main.test_truncate2_{file_type}",
        Path(f"load_data/test.{file_type}"),
        LoadMode.TRUNCATE,
    )
    df = dc.sql.run_query(f"select id, data from main.test_truncate2_{file_type}")
    assert_frame_equal(data, df)


def test_load_file_append(dc: DataCheck, file_type: str):
    dc.sql.table_loader.load_table_from_file(
        f"main.test_append_{file_type}",
        Path(f"load_data/test.{file_type}"),
        LoadMode.TRUNCATE,
    )
    dc.sql.table_loader.load_table_from_file(
        f"main.test_append_{file_type}",
        Path(f"load_data/test.{file_type}"),
        LoadMode.APPEND,
    )
    df = dc.sql.run_query(f"select id, data from main.test_append_{file_type}")
    check_data = pd.DataFrame.from_dict(
        {"id": [0, 1, 2, 0, 1, 2], "data": ["a", "b", "c", "a", "b", "c"]}
    )
    assert_frame_equal(check_data, df)
    assert len(df) == 6


def test_load_file_append_with_table(dc: DataCheck, file_type: str):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    create_test_table(f"test_append2_{file_type}", "main", dc)
    dc.sql.table_loader.load_table_from_file(
        f"main.test_append2_{file_type}",
        Path(f"load_data/test.{file_type}"),
        LoadMode.APPEND,
    )
    df = dc.sql.run_query(f"select id, data from main.test_append2_{file_type}")
    assert_frame_equal(data, df)
    assert len(df) == 3


def test_load_file_date_type(dc: DataCheck, file_type: str):
    create_test_table_with_date(f"test_date_{file_type}", "main", dc)
    dc.sql.table_loader.load_table_from_file(
        f"main.test_date_{file_type}",
        Path(f"load_data/test_date.{file_type}"),
        LoadMode.TRUNCATE,
    )
    df = dc.sql.run_query(f"select id, data, dat from main.test_date_{file_type}")
    dat = df.dat
    assert not dat.empty


def test_load_file_date_type_huge_date(dc: DataCheck, file_type: str):
    data = pd.DataFrame.from_dict(
        {
            "id": [0, 1],
            "data": ["a", "b"],
            "dat": [datetime.datetime(2021, 1, 25), datetime.datetime(9999, 12, 31)],
        }
    )
    create_test_table_with_datetime(f"test_date_huge_{file_type}", "main", dc)
    dc.sql.table_loader.load_table_from_file(
        f"main.test_date_huge_{file_type}",
        Path(f"load_data/test_date_huge.{file_type}"),
        LoadMode.TRUNCATE,
    )
    df = dc.sql.run_query(f"select id, data, dat from main.test_date_huge_{file_type}")
    assert_frame_equal(data, df)


def test_load_file_datetime_type(dc: DataCheck, file_type: str):
    create_test_table_with_datetime(f"test_datetime_{file_type}", "main", dc)
    dc.sql.table_loader.load_table_from_file(
        f"main.test_datetime_{file_type}",
        Path(f"load_data/test_datetime.{file_type}"),
        LoadMode.TRUNCATE,
    )
    df = dc.sql.run_query(f"select id, data, dat from main.test_datetime_{file_type}")
    dat = df.dat
    assert not dat.empty


def test_load_file_date_with_existing_table_replace(dc: DataCheck, file_type: str):
    create_test_table_with_datetime(f"test_date_replace_{file_type}", "main", dc)
    dc.sql.table_loader.load_table_from_file(
        f"main.test_date_replace_{file_type}",
        Path(f"load_data/test_date.{file_type}"),
        LoadMode.REPLACE,
    )
    df = dc.sql.run_query(
        f"select id, data, dat from main.test_date_replace_{file_type}"
    )
    if dc.sql.dialect == "oracle":
        # in Oracle this is a date type
        assert df.dat.dtype == "<M8[ns]"
    else:
        assert df.dat.dtype == "datetime64[ns]"


def test_load_file_decimal_type(dc: DataCheck, file_type: str):
    data = pd.DataFrame.from_dict(
        {"id": [0, 1], "data": ["a", "b"], "decim": [0.1, 0.2]}
    )
    create_test_table_with_decimal(f"test_decimals_{file_type}", "main", dc)
    dc.sql.table_loader.load_table_from_file(
        f"main.test_decimals_{file_type}",
        Path(f"load_data/test_decimals.{file_type}"),
        LoadMode.TRUNCATE,
    )
    df = dc.sql.run_query(f"select id, data, decim from main.test_decimals_{file_type}")
    assert_frame_equal(data, df)


def test_load_file_less_columns_in_file(dc: DataCheck, file_type: str):
    data = pd.DataFrame.from_dict(
        {"id": [0, 1, 2], "data": ["a", "b", "c"], "decim": [pd.NA, pd.NA, pd.NA]}
    )
    create_test_table_with_decimal(f"test_less_columns_in_{file_type}", "main", dc)
    dc.sql.table_loader.load_table_from_file(
        f"main.test_less_columns_in_{file_type}",
        Path(f"load_data/test.{file_type}"),
        LoadMode.TRUNCATE,
    )
    df = dc.sql.run_query(
        f"select id, data, decim from main.test_less_columns_in_{file_type}"
    )
    assert_frame_equal(data, df)


def test_load_file_more_columns_in_file(dc: DataCheck, file_type: str):
    create_test_table(f"test_more_columns_in_{file_type}", "main", dc)
    with pytest.raises(Exception):
        dc.sql.table_loader.load_table_from_file(
            f"main.test_more_columns_in_{file_type}",
            Path(f"load_data/test_decimals.{file_type}"),
            LoadMode.TRUNCATE,
        )


def test_table_exists(dc: DataCheck):
    create_test_table("test_table_exists", "main", dc)
    assert dc.sql.table_loader.table_exists("test_table_exists", "main")


def test_table_exists_non_existing(dc: DataCheck):
    assert not dc.sql.table_loader.table_exists(
        "test_table_exists_non_existing", "main"
    )


def test_drop_table_if_exists_with_existing_table(dc: DataCheck):
    create_test_table("test_drop_existing", "main", dc)
    dc.sql.table_loader.drop_table_if_exists("test_drop_existing", "main")
    with pytest.raises(Exception):
        dc.sql.run_query("select * from main.test_drop_existing")


def test_drop_table_if_exists_with_non_existing_table(dc: DataCheck):
    dc.sql.table_loader.drop_table_if_exists("test_drop_non_existing", "main")
    with pytest.raises(Exception):
        dc.sql.run_query("select * from temp.test_drop_non_existing")

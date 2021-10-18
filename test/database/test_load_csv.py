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


def test_load_csv_replace(dc: DataCheck):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    dc.sql.table_loader.load_table_from_csv_file(
        "main.test_replace", Path("load_data/test.csv"), LoadMode.REPLACE
    )
    df = dc.sql.run_query("select id, data from main.test_replace")
    assert_frame_equal(data, df)


def test_load_csv_replace_with_table(dc: DataCheck):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    create_test_table("test_replace2", "main", dc)
    dc.sql.table_loader.load_table_from_csv_file(
        "main.test_replace2", Path("load_data/test.csv"), LoadMode.REPLACE
    )
    df = dc.sql.run_query("select id, data from main.test_replace2")
    assert_frame_equal(data, df)


def test_load_csv_truncate(dc: DataCheck):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    dc.sql.table_loader.load_table_from_csv_file(
        "main.test_truncate", Path("load_data/test.csv"), LoadMode.TRUNCATE
    )
    df = dc.sql.run_query("select id, data from main.test_truncate")
    assert_frame_equal(data, df)


def test_load_csv_truncate_with_table(dc: DataCheck):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    create_test_table("test_truncate2", "main", dc)
    dc.sql.table_loader.load_table_from_csv_file(
        "main.test_truncate2", Path("load_data/test.csv"), LoadMode.TRUNCATE
    )
    df = dc.sql.run_query("select id, data from main.test_truncate2")
    assert_frame_equal(data, df)


def test_load_csv_append(dc: DataCheck):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    dc.sql.table_loader.load_table_from_csv_file(
        "main.test_append", Path("load_data/test.csv"), LoadMode.TRUNCATE
    )
    dc.sql.table_loader.load_table_from_csv_file(
        "main.test_append", Path("load_data/test.csv"), LoadMode.APPEND
    )
    df = dc.sql.run_query("select id, data from main.test_append")
    check_data = pd.DataFrame.from_dict(
        {"id": [0, 1, 2, 0, 1, 2], "data": ["a", "b", "c", "a", "b", "c"]}
    )
    assert_frame_equal(check_data, df)
    assert len(df) == 6


def test_load_csv_append_with_table(dc: DataCheck):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    create_test_table("test_append2", "main", dc)
    dc.sql.table_loader.load_table_from_csv_file(
        "main.test_append2", Path("load_data/test.csv"), LoadMode.APPEND
    )
    df = dc.sql.run_query("select id, data from main.test_append2")
    assert_frame_equal(data, df)
    assert len(df) == 3


def test_load_csv_date_type(dc: DataCheck):
    create_test_table_with_date("test_date", "main", dc)
    dc.sql.table_loader.load_table_from_csv_file(
        "main.test_date", Path("load_data/test_date.csv"), LoadMode.TRUNCATE
    )
    df = dc.sql.run_query("select id, data, dat from main.test_date")
    dat = df.dat
    assert not dat.empty


def test_load_csv_date_type_huge_date(dc: DataCheck):
    data = pd.DataFrame.from_dict(
        {
            "id": [0, 1],
            "data": ["a", "b"],
            "dat": [datetime.datetime(2021, 1, 25), datetime.datetime(9999, 12, 31)],
        }
    )
    create_test_table_with_datetime("test_date_huge", "main", dc)
    dc.sql.table_loader.load_table_from_csv_file(
        "main.test_date_huge", Path("load_data/test_date_huge.csv"), LoadMode.TRUNCATE
    )
    df = dc.sql.run_query("select id, data, dat from main.test_date_huge")
    assert_frame_equal(data, df)


def test_load_csv_datetime_type(dc: DataCheck):
    create_test_table_with_datetime("test_datetime", "main", dc)
    dc.sql.table_loader.load_table_from_csv_file(
        "main.test_datetime", Path("load_data/test_datetime.csv"), LoadMode.TRUNCATE
    )
    df = dc.sql.run_query("select id, data, dat from main.test_datetime")
    dat = df.dat
    assert not dat.empty


def test_load_csv_date_with_existing_table_replace(dc: DataCheck):
    create_test_table_with_datetime("test_date_replace", "main", dc)
    dc.sql.table_loader.load_table_from_csv_file(
        "main.test_date_replace", Path("load_data/test_date.csv"), LoadMode.REPLACE
    )
    df = dc.sql.run_query("select id, data, dat from main.test_date_replace")
    if dc.sql.dialect == "oracle":
        # in Oracle this is a date type
        assert df.dat.dtype == "<M8[ns]"
    else:
        assert df.dat.dtype == "datetime64[ns]"


def test_load_csv_decimal_type(dc: DataCheck):
    data = pd.DataFrame.from_dict(
        {"id": [0, 1], "data": ["a", "b"], "decim": [0.1, 0.2]}
    )
    create_test_table_with_decimal("test_decimals", "main", dc)
    dc.sql.table_loader.load_table_from_csv_file(
        "main.test_decimals", Path("load_data/test_decimals.csv"), LoadMode.TRUNCATE
    )
    df = dc.sql.run_query("select id, data, decim from main.test_decimals")
    assert_frame_equal(data, df)


def test_load_csv_less_columns_in_csv(dc: DataCheck):
    data = pd.DataFrame.from_dict(
        {"id": [0, 1, 2], "data": ["a", "b", "c"], "decim": [pd.NA, pd.NA, pd.NA]}
    )
    create_test_table_with_decimal("test_less_columns_in_csv", "main", dc)
    dc.sql.table_loader.load_table_from_csv_file(
        "main.test_less_columns_in_csv", Path("load_data/test.csv"), LoadMode.TRUNCATE
    )
    df = dc.sql.run_query("select id, data, decim from main.test_less_columns_in_csv")
    assert_frame_equal(data, df)


def test_load_csv_more_columns_in_csv(dc: DataCheck):
    create_test_table("test_more_columns_in_csv", "main", dc)
    with pytest.raises(Exception):
        dc.sql.table_loader.load_table_from_csv_file(
            "main.test_more_columns_in_csv",
            Path("load_data/test_decimals.csv"),
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

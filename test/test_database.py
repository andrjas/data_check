import sys
import os
from pathlib import Path
from pandas.core.frame import DataFrame
import pytest
import pandas as pd
from sqlalchemy import Table, Column, String, Integer, MetaData, Date, Numeric, DateTime
import datetime

my_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, my_path + "/../")

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
    return _dc


def create_test_table(table_name: str, schema: str, dc: DataCheck):
    if dc.sql.dialect == "oracle":
        dc.sql.run_sql(
            f"create table {schema}.{table_name} (id number(10), data varchar2(10))"
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


@pytest.fixture
def data_types_check(dc: DataCheck):
    res = dc.run_test(Path("checks/basic/data_types.sql"), return_all=True)
    assert isinstance(res.result, DataFrame)
    assert not res.result.empty
    return res.result.iloc[0]


def assert_equal_df(df1: pd.DataFrame, df2: pd.DataFrame):
    df_diff = df1.merge(df2, how="outer", indicator=True)
    assert df_diff[df_diff["_merge"] != "both"].empty


def test_data_types_string(data_types_check):
    assert data_types_check.string_test == "string"


def test_data_types_int(data_types_check):
    assert data_types_check.int_test == 42


def test_data_types_float(data_types_check):
    assert data_types_check.float_test == 42.1


def test_data_types_date(data_types_check, dc: DataCheck):
    # if dc.sql.dialect == "sqlite":
    #     # sqlite doesn't have a date type
    #     assert data_types_check.date_test == "2020-12-20"
    # else:
    assert data_types_check.date_test == datetime.date(2020, 12, 20)


def test_data_types_null(data_types_check):
    assert pd.isna(data_types_check.null_test)


def test_data_types_empty_string(data_types_check):
    """Empty strings from the database must be converted to NA to match CSV encoding."""
    assert pd.isna(data_types_check.empty_string_test)


def test_data_types_whitespace(data_types_check):
    """Whitespace must stay the same, not converted to NA."""
    assert data_types_check.whitespace_test == "   "


def test_float_decimal_conversion(dc: DataCheck):
    res = dc.run_test(Path("checks/basic/float.sql"))
    assert res


def test_unicode(dc: DataCheck):
    res = dc.run_test(Path("checks/basic/unicode_string.sql"))
    assert res


def test_decimal_varchar(dc: DataCheck):
    """
    Test a varchar column, that has only decimals in the csv file
    """
    res = dc.run_test(Path("checks/basic/decimal_varchar.sql"))
    assert res


def test_sorted_set(dc: DataCheck):
    res = dc.run_test(Path("checks/basic/sorted_set.sql"))
    assert res


def test_load_csv_replace(dc: DataCheck):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    dc.sql.load_table_from_csv_file(
        "temp.test_replace", Path("load_data/test.csv"), LoadMode.REPLACE
    )
    df = dc.sql.run_query("select id, data from temp.test_replace")
    assert_equal_df(data, df)


def test_load_csv_replace_with_table(dc: DataCheck):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    create_test_table("test_replace2", "temp", dc)
    dc.sql.load_table_from_csv_file(
        "temp.test_replace2", Path("load_data/test.csv"), LoadMode.REPLACE
    )
    df = dc.sql.run_query("select id, data from temp.test_replace2")
    assert_equal_df(data, df)


def test_load_csv_truncate(dc: DataCheck):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    dc.sql.load_table_from_csv_file(
        "temp.test_truncate", Path("load_data/test.csv"), LoadMode.TRUNCATE
    )
    df = dc.sql.run_query("select id, data from temp.test_truncate")
    assert_equal_df(data, df)


def test_load_csv_truncate_with_table(dc: DataCheck):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    create_test_table("test_truncate2", "temp", dc)
    dc.sql.load_table_from_csv_file(
        "temp.test_truncate2", Path("load_data/test.csv"), LoadMode.TRUNCATE
    )
    df = dc.sql.run_query("select id, data from temp.test_truncate2")
    assert_equal_df(data, df)


def test_load_csv_append(dc: DataCheck):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    dc.sql.load_table_from_csv_file(
        "temp.test_append", Path("load_data/test.csv"), LoadMode.TRUNCATE
    )
    dc.sql.load_table_from_csv_file(
        "temp.test_append", Path("load_data/test.csv"), LoadMode.APPEND
    )
    df = dc.sql.run_query("select id, data from temp.test_append")
    # since the same data is loaded twice,
    # the merge will also work on one copy of the data
    assert_equal_df(data, df)
    assert len(df) == 6


def test_load_csv_append_with_table(dc: DataCheck):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    create_test_table("test_append2", "temp", dc)
    dc.sql.load_table_from_csv_file(
        "temp.test_append2", Path("load_data/test.csv"), LoadMode.APPEND
    )
    df = dc.sql.run_query("select id, data from temp.test_append2")
    assert_equal_df(data, df)
    assert len(df) == 3


def test_load_csv_date_type(dc: DataCheck):
    create_test_table_with_date("test_date", "temp", dc)
    dc.sql.load_table_from_csv_file(
        "temp.test_date", Path("load_data/test_date.csv"), LoadMode.TRUNCATE
    )
    df = dc.sql.run_query("select id, data, dat from temp.test_date")
    dat = df.dat
    assert not dat.empty


def test_load_csv_datetime_type(dc: DataCheck):
    create_test_table_with_datetime("test_datetime", "temp", dc)
    dc.sql.load_table_from_csv_file(
        "temp.test_datetime", Path("load_data/test_datetime.csv"), LoadMode.TRUNCATE
    )
    df = dc.sql.run_query("select id, data, dat from temp.test_datetime")
    dat = df.dat
    assert not dat.empty


def test_load_csv_date_with_existing_table_replace(dc: DataCheck):
    create_test_table_with_date("test_date_replace", "temp", dc)
    dc.sql.load_table_from_csv_file(
        "temp.test_date_replace", Path("load_data/test_date.csv"), LoadMode.REPLACE
    )
    df = dc.sql.run_query("select id, data, dat from temp.test_date_replace")
    if dc.sql.dialect == "oracle":
        # in Oracle this is a date type
        assert df.dat.dtype == "<M8[ns]"
    else:
        assert df.dat.dtype == "object"


def test_load_csv_decimal_type(dc: DataCheck):
    data = pd.DataFrame.from_dict(
        {"id": [0, 1], "data": ["a", "b"], "decim": [0.1, 0.2]}
    )
    create_test_table_with_decimal("test_decimals", "temp", dc)
    dc.sql.load_table_from_csv_file(
        "temp.test_decimals", Path("load_data/test_decimals.csv"), LoadMode.TRUNCATE
    )
    df = dc.sql.run_query("select id, data, decim from temp.test_decimals")
    assert_equal_df(data, df)


def test_load_csv_less_columns_in_csv(dc: DataCheck):
    data = pd.DataFrame.from_dict(
        {"id": [0, 1, 2], "data": ["a", "b", "c"], "decim": [pd.NA, pd.NA, pd.NA]}
    )
    create_test_table_with_decimal("test_less_columns_in_csv", "temp", dc)
    dc.sql.load_table_from_csv_file(
        "temp.test_less_columns_in_csv", Path("load_data/test.csv"), LoadMode.TRUNCATE
    )
    df = dc.sql.run_query("select id, data, decim from temp.test_less_columns_in_csv")
    assert_equal_df(data, df)


def test_load_csv_more_columns_in_csv(dc: DataCheck):
    create_test_table("test_more_columns_in_csv", "temp", dc)
    with pytest.raises(Exception):
        dc.sql.load_table_from_csv_file(
            "temp.test_more_columns_in_csv",
            Path("load_data/test_decimals.csv"),
            LoadMode.TRUNCATE,
        )


def test_dialect(dc: DataCheck):
    assert dc.sql.dialect in ["sqlite", "postgresql", "mysql", "mssql", "oracle"]


def test_table_exists(dc: DataCheck):
    create_test_table("test_table_exists", "temp", dc)
    assert dc.sql.table_exists("test_table_exists", "temp")


def test_table_exists_non_existing(dc: DataCheck):
    assert not dc.sql.table_exists("test_table_exists_non_existing", "temp")


def test_drop_table_if_exists_with_existing_table(dc: DataCheck):
    create_test_table("test_drop_existing", "temp", dc)
    dc.sql.drop_table_if_exists("test_drop_existing", "temp")
    with pytest.raises(Exception):
        dc.sql.run_query("select * from temp.test_drop_existing")


def test_drop_table_if_exists_with_non_existing_table(dc: DataCheck):
    dc.sql.drop_table_if_exists("test_drop_non_existing", "temp")
    with pytest.raises(Exception):
        dc.sql.run_query("select * from temp.test_drop_non_existing")

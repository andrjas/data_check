import sys
import os
from pathlib import Path
import pytest
import pandas as pd
from sqlalchemy import Table, Column, String, Integer, MetaData

my_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, my_path + "/../")

from data_check import DataCheck  # noqa E402
from data_check.config import DataCheckConfig  # noqa E402
from data_check.sql import LoadMethod

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


@pytest.fixture
def data_types_check(dc: DataCheck):
    res = dc.run_test(Path("checks/basic/data_types.sql"), return_all=True)
    assert not res.result.empty
    return res.result.iloc[0]


def assert_equal_df(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
    df_diff = df1.merge(df2, how="outer", indicator=True)
    assert df_diff[df_diff["_merge"] != "both"].empty


def test_data_types_string(data_types_check):
    assert data_types_check.string_test == "string"


def test_data_types_int(data_types_check):
    assert data_types_check.int_test == 42


def test_data_types_float(data_types_check):
    assert data_types_check.float_test == 42.1


def test_data_types_date(data_types_check):
    assert data_types_check.date_test == "2020-12-20"


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
        "temp.test_replace", Path("load_data/test.csv"), LoadMethod.REPLACE
    )
    df = dc.sql.run_query("select id, data from temp.test_replace")
    assert_equal_df(data, df)


def test_load_csv_replace_with_table(dc: DataCheck):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    create_test_table("test_replace2", "temp", dc)
    dc.sql.load_table_from_csv_file(
        "temp.test_replace2", Path("load_data/test.csv"), LoadMethod.REPLACE
    )
    df = dc.sql.run_query("select id, data from temp.test_replace2")
    assert_equal_df(data, df)


def test_load_csv_truncate(dc: DataCheck):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    dc.sql.load_table_from_csv_file(
        "temp.test_truncate", Path("load_data/test.csv"), LoadMethod.TRUNCATE
    )
    df = dc.sql.run_query("select id, data from temp.test_truncate")
    assert_equal_df(data, df)


def test_load_csv_truncate_with_table(dc: DataCheck):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    create_test_table("test_truncate2", "temp", dc)
    dc.sql.load_table_from_csv_file(
        "temp.test_truncate2", Path("load_data/test.csv"), LoadMethod.TRUNCATE
    )
    df = dc.sql.run_query("select id, data from temp.test_truncate2")
    assert_equal_df(data, df)


def test_load_csv_append(dc: DataCheck):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    dc.sql.load_table_from_csv_file(
        "temp.test_append", Path("load_data/test.csv"), LoadMethod.TRUNCATE
    )
    dc.sql.load_table_from_csv_file(
        "temp.test_append", Path("load_data/test.csv"), LoadMethod.APPEND
    )
    df = dc.sql.run_query("select id, data from temp.test_append")
    assert_equal_df(
        data, df
    )  # since the same data is loaded twice, the merge will also work on one copy of the data
    assert len(df) == 6


def test_load_csv_append_with_table(dc: DataCheck):
    data = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    create_test_table("test_append2", "temp", dc)
    dc.sql.load_table_from_csv_file(
        "temp.test_append2", Path("load_data/test.csv"), LoadMethod.APPEND
    )
    df = dc.sql.run_query("select id, data from temp.test_append2")
    assert_equal_df(data, df)
    assert len(df) == 3


def test_dialect(dc: DataCheck):
    assert dc.sql.dialect in ["sqlite", "postgresql", "mysql", "mssql", "oracle"]

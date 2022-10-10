from pathlib import Path
from pandas.core.frame import DataFrame
import pytest
import pandas as pd
import datetime
from sqlalchemy import Table, Column, String, Integer, MetaData


from data_check import DataCheck  # noqa E402

# These tests should work on any database.
# The tests are generic, but in integration tests each database uses specific SQL files.


@pytest.fixture
def data_types_check(dc: DataCheck):
    res = dc.get_check(Path("checks/basic/data_types.sql")).run_test()
    assert isinstance(res.result, DataFrame)
    assert not res.full_result.empty
    return res.full_result.iloc[0]


def create_test_table_db(table_name: str, schema: str, dc: DataCheck):
    dc.sql.table_loader.drop_table_if_exists(table_name, schema)
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


def test_data_types_string(data_types_check):
    assert data_types_check.string_test == "string"


def test_data_types_int(data_types_check):
    assert data_types_check.int_test == 42


def test_data_types_float(data_types_check):
    assert data_types_check.float_test == 42.1


def test_data_types_date(data_types_check):
    assert data_types_check.date_test == datetime.datetime(2020, 12, 20)


@pytest.mark.skip(
    reason="pandas automatically infers this type to timestamp. Skipped, until this feature is removed."
)
def test_data_types_date_is_datetime_type(data_types_check):
    assert type(data_types_check.date_test) == datetime.datetime


def test_data_types_huge_date(data_types_check):
    assert data_types_check.inf_date_test == datetime.datetime(9999, 12, 31)


def test_data_types_huge_date_is_datetime_type(data_types_check):
    assert type(data_types_check.inf_date_test) == datetime.datetime


def test_data_types_null(data_types_check):
    assert pd.isna(data_types_check.null_test)


def test_data_types_empty_string(data_types_check):
    """Empty strings from the database must be converted to NA to match CSV encoding."""
    assert pd.isna(data_types_check.empty_string_test)


def test_data_types_whitespace(data_types_check):
    """Whitespace must stay the same, not converted to NA."""
    assert data_types_check.whitespace_test == "   "


def test_float_decimal_conversion(dc: DataCheck):
    res = dc.get_check(Path("checks/basic/float.sql")).run_test()
    assert res


def test_unicode(dc: DataCheck):
    res = dc.get_check(Path("checks/basic/unicode_string.sql")).run_test()
    assert res


def test_decimal_varchar(dc: DataCheck):
    """
    Test a varchar column, that has only decimals in the csv file
    """
    res = dc.get_check(Path("checks/basic/decimal_varchar.sql")).run_test()
    assert res


def test_sorted_set(dc: DataCheck):
    res = dc.get_check(Path("checks/basic/sorted_set.sql")).run_test()
    assert res


def test_dialect(dc: DataCheck):
    assert dc.sql.dialect in ("sqlite", "postgresql", "mysql", "mssql", "oracle")


def test_leading_zeros_string(dc: DataCheck):
    res = dc.get_check(Path("checks/basic/leading_zeros.sql")).run_test()
    assert res


def test_lookup_bindings(dc: DataCheck):
    dc.load_lookups()
    res = dc.get_check(Path("checks/templates/binding.sql")).run_test()
    print(res)
    assert res

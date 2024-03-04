import datetime
from pathlib import Path

import pandas as pd
import pytest
from pandas.core.frame import DataFrame

from data_check import DataCheck  # noqa E402

# These tests should work on any database.
# The tests are generic, but in integration tests each database uses specific SQL files.


@pytest.fixture()
def data_types_check(dc: DataCheck):
    check = dc.get_check(Path("checks/basic/data_types.sql"))
    assert check
    res = check.run_test()
    assert isinstance(res.result, DataFrame)
    assert isinstance(res.full_result, DataFrame)
    assert not res.full_result.empty
    return res.full_result.iloc[0]


def test_data_types_string(data_types_check):
    assert data_types_check.string_test == "string"


def test_data_types_int(data_types_check):
    int_test_value = 42
    assert data_types_check.int_test == int_test_value


def test_data_types_float(data_types_check):
    float_test_value = 42.1
    assert data_types_check.float_test == float_test_value


def test_data_types_date(data_types_check):
    assert data_types_check.date_test == datetime.datetime(2020, 12, 20)


def test_data_types_date_is_datetime_type(data_types_check):
    assert type(data_types_check.date_test) == pd.Timestamp


def test_data_types_huge_date(data_types_check):
    assert data_types_check.inf_date_test == datetime.datetime(9999, 12, 31)


def test_data_types_huge_date_is_timestamp_type(data_types_check):
    assert type(data_types_check.inf_date_test) == pd.Timestamp


def test_data_types_null(data_types_check):
    assert pd.isna(data_types_check.null_test)


def test_data_types_empty_string(data_types_check):
    """Empty strings from the database must be converted to NA to match CSV encoding."""
    assert pd.isna(data_types_check.empty_string_test)


def test_data_types_whitespace(data_types_check):
    """Whitespace must stay the same, not converted to NA."""
    assert data_types_check.whitespace_test == "   "


def test_float_decimal_conversion(dc: DataCheck):
    check = dc.get_check(Path("checks/basic/float.sql"))
    assert check
    res = check.run_test()
    assert res


def test_unicode(dc: DataCheck):
    check = dc.get_check(Path("checks/basic/unicode_string.sql"))
    assert check
    res = check.run_test()
    assert res


def test_decimal_varchar(dc: DataCheck):
    """
    Test a varchar column, that has only decimals in the csv file
    """
    check = dc.get_check(Path("checks/basic/decimal_varchar.sql"))
    assert check
    res = check.run_test()
    assert res


def test_sorted_set(dc: DataCheck):
    check = dc.get_check(Path("checks/basic/sorted_set.sql"))
    assert check
    res = check.run_test()
    assert res


def test_dialect(dc: DataCheck):
    assert dc.sql.dialect in (
        "sqlite",
        "postgresql",
        "mysql",
        "mssql",
        "oracle",
        "duckdb",
        "databricks",
    )


def test_leading_zeros_string(dc: DataCheck):
    check = dc.get_check(Path("checks/basic/leading_zeros.sql"))
    assert check
    res = check.run_test()
    assert res


def test_lookup_bindings(dc: DataCheck):
    if dc.sql.dialect == "databricks":
        pytest.skip("databricks doesn't support lookups yet")
    dc.load_lookups()
    check = dc.get_check(Path("checks/templates/binding.sql"))
    assert check
    res = check.run_test()
    print(res)
    assert res

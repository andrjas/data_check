import sys
import os
from pathlib import Path
import pytest
import pandas as pd

my_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, my_path + "/../")

from data_check import DataCheck  # noqa E402

# These tests should work on any database.
# The tests are generic, but in integration tests each database uses specific SQL files.


@pytest.fixture
def dc():
    config = DataCheck.read_config()
    return DataCheck(config.get("connections", {}).get("test"))


@pytest.fixture
def data_types_check(dc):
    res = dc.run_test(Path("checks/basic/data_types.sql"), return_all=True)
    assert not res.result.empty
    return res.result.iloc[0]


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


def test_float_decimal_conversion(dc):
    res = dc.run_test(Path("checks/basic/float.sql"))
    assert res


def test_unicode(dc):
    res = dc.run_test(Path("checks/basic/unicode_string.sql"))
    assert res

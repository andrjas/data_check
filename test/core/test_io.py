import os
from pathlib import Path

import pandas as pd
import pytest

from data_check.exceptions import DataCheckException
from data_check.io import expand_files, get_expect_file, read_csv, read_sql_file


def test_expand_files():
    files = expand_files([Path("checks/basic"), Path("checks/failing")])
    assert len(files) >= 3


def test_expand_files_csv():
    files = expand_files([Path("load_data")], extension=".csv")
    assert len(files) >= 3


def test_expand_files_empty():
    files = expand_files([Path("load_data")])
    assert len(files) == 0


def test_expand_files_not_existing():
    with pytest.raises(Exception) as e:
        expand_files([Path("checks/non_existing")])
    assert str(e.value) == f"unexpected path: checks{os.sep}non_existing"


def test_read_sql_file_with_template():
    text = read_sql_file(Path("checks/templates/template1.sql"), {})
    assert "{{" not in text


def test_get_expect_file():
    ef = get_expect_file(Path("test_file.sql"))
    assert ef == Path("test_file.csv")


def test_get_expect_file_null():
    p = Path()
    ef = get_expect_file(p)
    assert ef == Path()


def test_get_expect_file_empty():
    p = Path("")
    ef = get_expect_file(p)
    assert ef == Path()


def test_get_expect_file_sql():
    p = Path(".sql")
    ef = get_expect_file(p)
    print(f"stem: {p.stem}")
    print(f"suffix: '{p.suffix}'")
    assert ef == Path()


def test_non_unicode_sql():
    with pytest.raises(DataCheckException):
        read_sql_file(Path("checks/failing/non_unicode.sql"), {})


def test_non_unicode_csv():
    with pytest.raises(DataCheckException):
        read_csv(Path("checks/failing/non_unicode.csv"))


def test_read_csv_date_columns_with_empty_values_return_nat():
    df = read_csv(Path("load_data/sample/test_date_with_null_dates.csv"))
    assert df["j"].iloc[1] is pd.NaT

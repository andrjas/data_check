from datetime import datetime

from data_check.date import (
    isoparse,
    parse_date_columns,
)


def test_isoparse_datetime():
    s = "2020-12-24 00:00:00"
    d = isoparse(s)
    assert isinstance(d, datetime)


def test_isoparse_date():
    s = "2020-12-24"
    d = isoparse(s)
    assert isinstance(d, datetime)


def test_isoparse_empty_string():
    s = ""
    d = isoparse(s)
    assert d == ""


def test_isoparse_empty_null():
    s = None
    d = isoparse(s)
    assert d == ""


def test_isoparse_datetime_huge_date():
    s = "9999-12-31 00:00:00"
    d = isoparse(s)
    assert d == datetime(9999, 12, 31, 0, 0, 0)


def test_isoparse_date_huge_date():
    s = "9999-12-31"
    d = isoparse(s)
    assert d == datetime(9999, 12, 31, 0, 0, 0)

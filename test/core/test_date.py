from datetime import datetime

import pandas as pd
from pandas.testing import assert_frame_equal

from data_check.date import isoparse, parse_date_columns


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


def test_parse_date_columns_without_dates():
    df = pd.DataFrame.from_dict(
        {
            "a": [1, 2, 3],
            "b": ["x", "y", "z"],
        }
    )
    date_columns, parsed_df = parse_date_columns(df)
    assert date_columns == []
    assert_frame_equal(df, parsed_df)


def test_parse_date_columns_with_dates():
    df = pd.DataFrame.from_dict(
        {
            "a": [1, 2, 3],
            "b": ["2020-12-20", "2020-10-10", "2020-11-11"],
        }
    )
    date_columns, parsed_df = parse_date_columns(df)
    assert date_columns == ["b"]
    assert_frame_equal(df, parsed_df)


def test_parse_date_columns_with_datetimes():
    df = pd.DataFrame.from_dict(
        {
            "a": [1, 2, 3],
            "b": ["2020-12-20 12:12:12", "2020-10-10 10:10:10", "2020-11-11 11:11:11"],
        }
    )
    date_columns, parsed_df = parse_date_columns(df)
    assert date_columns == ["b"]
    assert_frame_equal(df, parsed_df)


def test_parse_date_columns_with_dates_and_others():
    df = pd.DataFrame.from_dict(
        {
            "a": [1, 2, 3],
            "b": ["2020-12-20", "2020-10-10", "0123456789"],
        }
    )
    date_columns, parsed_df = parse_date_columns(df)
    assert date_columns == []
    assert_frame_equal(df, parsed_df)


def test_parse_date_columns_with_datetimes_and_others():
    df = pd.DataFrame.from_dict(
        {
            "a": [1, 2, 3],
            "b": ["2020-12-20 12:12:12", "2020-10-10 10:10:10", "0123456789"],
        }
    )
    date_columns, parsed_df = parse_date_columns(df)
    assert date_columns == []
    assert_frame_equal(df, parsed_df)


def test_parse_date_columns_with_dates_and_empty_at_end():
    df = pd.DataFrame.from_dict(
        {
            "a": [1, 2, 3],
            "b": ["2020-12-20", "2020-10-10", ""],
        }
    )
    date_columns, parsed_df = parse_date_columns(df)
    assert date_columns == ["b"]
    assert_frame_equal(df, parsed_df)


def test_parse_date_columns_with_datetimes_and_empty_at_end():
    df = pd.DataFrame.from_dict(
        {
            "a": [1, 2, 3],
            "b": ["2020-12-20 12:12:12", "2020-10-10 10:10:10", ""],
        }
    )
    date_columns, parsed_df = parse_date_columns(df)
    assert date_columns == ["b"]
    assert_frame_equal(df, parsed_df)

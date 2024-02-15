from datetime import datetime

import pandas as pd
import pytest
import pytz
from pandas.testing import assert_frame_equal

from data_check.checks.sql_base_check import SQLBaseCheck
from data_check.date import parse_date_columns


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
            "b": ["2020-12-20", "2020-10-10", "0123498765"],
        }
    )
    date_columns, parsed_df = parse_date_columns(df)
    assert date_columns == []
    assert_frame_equal(df, parsed_df)


def test_parse_date_columns_with_datetimes_and_others():
    df = pd.DataFrame.from_dict(
        {
            "a": [1, 2, 3],
            "b": ["2020-12-20 12:12:12", "2020-10-10 10:10:10", "0123498765"],
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


def test_mixed_tzinfo_datetime():
    dt1 = datetime(2020, 12, 20, 12, 12, 12, tzinfo=pytz.UTC)
    dt2 = datetime(2020, 12, 20, 12, 12, 12)
    df1 = pd.DataFrame.from_dict({"a": [1], "d": [dt1]})
    df2 = pd.DataFrame.from_dict({"a": [1], "d": [dt2]})
    SQLBaseCheck.convert_mixed_tzinfo_columns(df1, df2)
    mr = SQLBaseCheck.merge_results(df1, df2)
    assert len(mr) == 1
    assert mr["_merge"].iloc[0] == "both"


test_dates = [
    ("2023-12-20", "2023-12-21"),
    ("2023-12-20", "9999-12-31"),
    ("8888-12-31", "9999-12-31"),
    ("2023-12-20", ""),
    ("9999-12-31", ""),
    ("9999-12-31", "2023-12-20"),
    ("", "2023-12-20"),
    ("", "9999-12-31"),
]


@pytest.mark.parametrize(("date1", "date2"), test_dates)
def test_result_different_dates(date1, date2):
    df1 = pd.DataFrame.from_dict({"a": [1], "d": [date1]})
    df2 = pd.DataFrame.from_dict({"a": [1], "d": [date2]})
    _, parsed_df1 = parse_date_columns(df1)
    _, parsed_df2 = parse_date_columns(df2)
    mr = SQLBaseCheck.merge_results(parsed_df1, parsed_df2)
    assert len(mr) == 2

from contextlib import suppress
from datetime import date, datetime
from typing import List, Tuple, cast

import pandas as pd


def is_possible_date_column(column: pd.Series) -> bool:
    """For a column to be a possible date it must have some non-empty values that are at least 10 characters long."""
    not_null = column.dropna()
    non_empty = not_null[not_null.astype(str).str.len() > 0]
    min_len_10 = non_empty[non_empty.astype(str).str.len() >= 10]
    return len(min_len_10) > 0


def parse_date_columns(df: pd.DataFrame) -> Tuple[List[str], pd.DataFrame]:
    """Tries to parse each column as a date.
    Returns a tuple with the list of the column names that were parsed as dates
    and the DataFrame with these columns replaced as Timestamp.
    """
    _date_columns: List[str] = []
    for column_name, column in df.items():
        if is_possible_date_column(column):
            try:
                _col = pd.to_datetime(column, errors="raise", format="ISO8601")
                df[column_name] = _col
                _date_columns.append(str(column_name))
            except pd._libs.tslibs.np_datetime.OutOfBoundsDatetime:
                with suppress(Exception):
                    _col_ts = column.apply(pd.Timestamp)
                    df[column_name] = _col_ts
                    _date_columns.append(str(column_name))
            except Exception:
                pass
    return _date_columns, df


def column_has_empty_values(column: pd.Series):
    empty_vals = column.where(column == "")
    return not empty_vals.empty


def is_date_column(column_name: str, column: pd.Series, dtype) -> bool:
    col_type = dtype.get(column_name)
    if col_type:
        return col_type.python_type in (datetime, date)

    # mixed date/datetime and str columns are assumed to be a date column too
    exp_val_types = set(
        [datetime, date, str]
    )  # numpy.datetime64 is not of interest here, since if it's already used, we assume that the frame is loaded correctly
    val_types = set(type(v) for v in column.values)
    return exp_val_types.issuperset(val_types)


def fix_date_dtype(df: pd.DataFrame, dtype):
    """Fixes isoparse parsed DataFrames inplace so they can be used in other pandas functions like to_sql.
    - converts None to NaT in date columns
    """
    if not dtype:
        dtype = {}
    for column_name, column in df.items():
        if is_date_column(cast(str, column_name), column, dtype):
            if column_has_empty_values(column):
                # reverse the operations from CSVCheck.get_result:
                column.replace({pd.NA: pd.NaT}, inplace=True)
                column.fillna(value=pd.NaT, inplace=True)  # type: ignore
                column.replace(r"^$", pd.NaT, regex=True, inplace=True)  # type: ignore

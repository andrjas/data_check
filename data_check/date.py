from contextlib import suppress
from datetime import datetime, date
from typing import List, Tuple, Union, cast
import pandas as pd
from dateutil.parser import isoparse as _isoparse
import math
import warnings


def parse_date_columns(df: pd.DataFrame) -> Tuple[List[str], pd.DataFrame]:
    """Tries to parse each column as a date.
    Returns a tuple with the list of the column names that were parsed as dates
    and the DataFrame with these columns replaced as datetime.
    """
    _date_columns: List[str] = []
    for column_name, column in df.items():
        # only try to convert, if some values exist in the column
        if not column.isna().all():
            with suppress(Exception):
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", category=FutureWarning)
                    _col = column.apply(isoparse, convert_dtype=False)
                df[column_name] = _col
                _date_columns.append(str(column_name))
    return _date_columns, df


def isoparse(column: Union[int, float, str, None]):
    if isinstance(column, float) and math.isnan(column):
        column = ""
    if column is None or not str(column):
        return ""
    if len(str(column)) < 10 or not isinstance(column, str):
        # must be at least of format YYYY-MM-DD to be a date
        raise ValueError(("VE", column, type(column)))
    res = _isoparse(str(column))
    return res


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

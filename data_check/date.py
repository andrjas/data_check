from typing import List, Tuple
import pandas as pd
from dateutil.parser import isoparse as _isoparse
import math


def parse_date_columns(df: pd.DataFrame) -> Tuple[List[str], pd.DataFrame]:
    """Tries to parse each column as a date.
    Returns a tuple with the list of the column names that were parsed as dates
    and the DataFrame with these columns replaced as datetime.
    """
    _date_columns = []
    for column_name, column in df.items():
        # only try to convert, if some values exist in the column
        if not column.isna().all():
            try:
                _col = column.apply(isoparse, convert_dtype=False)
                df[column_name] = _col
                _date_columns.append(str(column_name))
            except Exception:
                pass
    return _date_columns, df


def isoparse(column):
    if isinstance(column, float) and math.isnan(column):
        column = ""
    if column is None or len(str(column)) == 0:
        return ""
    if len(str(column)) < 10 or not isinstance(column, str):
        # must be at least of format YYYY-MM-DD to be a date
        raise ValueError(("VE", column, type(column)))
    res = _isoparse(str(column))
    return res
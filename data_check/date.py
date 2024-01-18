from contextlib import suppress
from typing import List, Tuple

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

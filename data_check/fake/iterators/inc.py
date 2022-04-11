from __future__ import annotations
from typing import TYPE_CHECKING, Union
import datetime
import numbers
import pandas as pd

if TYPE_CHECKING:
    from ..column_config import ColumnConfig


def date_inc(x: datetime.date):
    return x + datetime.timedelta(days=1)


def number_inc(x: Union[int, float]):
    return x + 1


def inc(column: ColumnConfig, data: pd.DataFrame):
    apply_func = None
    if column.python_type in (datetime.date, datetime.datetime):
        apply_func = date_inc
    elif issubclass(column.python_type, numbers.Number):
        apply_func = number_inc
    else:
        raise Exception(f"unsupported type for inc: {column.python_type.__name__}")

    if apply_func:
        data[column.name] = data[column.name].apply(apply_func)

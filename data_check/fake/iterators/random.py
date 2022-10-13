from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from ..column_config import ColumnConfig


def random_apply(_: Any, column: ColumnConfig):
    # Just run the generator for the column again to get some random value.
    return column.generate()


def random(column: ColumnConfig, data: pd.DataFrame):
    apply_func = random_apply
    data[column.name] = data[column.name].apply(apply_func, column=column)

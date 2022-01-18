import pandas as pd
import itertools
from typing import List, Tuple

from data_check.result import DataCheckResult


def _df_same_uniques(df: pd.DataFrame, columns: List[str]) -> bool:
    return len(df.drop_duplicates()) == len(df[columns].drop_duplicates())


def get_diffed_df(df: pd.DataFrame, result: DataCheckResult):
    """Tries to remove columns from df, while preserving uniqueness
    across result.full_result. The first column is always preserved.
    """
    if result.full_result is None:
        return df
    full_df = result.full_result
    columns: List[str] = list(full_df.columns)
    # remove _merge from checking and append it at the end
    columns.remove("_merge")
    can_remove = columns[1:]  # always keep the first column
    # construct all possible combinations of columns
    can_remove_combinations = [
        comb
        for iter_len in range(1, len(can_remove) + 1)
        for comb in itertools.combinations(can_remove, iter_len)
    ]
    columns_to_remove: List[str] = []
    # check which combinations keep the DataFrame of same size,
    # and remove the combination with the most columns
    for _cols in sorted(can_remove_combinations, key=len, reverse=True):
        _column_part = list(set(columns) - set(_cols))
        if _df_same_uniques(full_df, _column_part):
            columns_to_remove = list(_cols)
            break

    for col in columns_to_remove:
        columns.remove(col)
    columns.append("_merge")

    return df[columns]

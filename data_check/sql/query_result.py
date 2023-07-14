from typing import List, Optional, cast

import pandas as pd
from pandas.api.types import is_string_dtype
from sqlalchemy.engine.cursor import CursorResult
from sqlalchemy.engine.row import Row

from ..date import parse_date_columns


class QueryResult:
    def __init__(self, query: str, result: Optional[CursorResult]):
        self.query = query
        self.data: List[Row] = []
        self.columns: List[str] = []
        self._df: Optional[pd.DataFrame] = None
        self._date_columns: List[str] = []

        if result is not None:
            self.data = cast(List[Row], result.fetchall())
            self.columns = cast(List[str], result.keys())

    @property
    def date_columns(self) -> List[str]:
        if self._df is None:
            self._load_df()
        return self._date_columns

    @property
    def string_columns(self) -> List[str]:
        return [
            k
            for k in cast(List[str], self.df.keys())
            if is_string_dtype(self.df[k]) and k not in self.date_columns
        ]

    def _load_df(self):
        # use coerce_float=True as it is used in pandas by default
        frame = pd.DataFrame.from_records(
            self.data, columns=self.columns, coerce_float=True
        )
        date_cols, df = parse_date_columns(frame)
        self._date_columns = date_cols
        self._df = df

    @property
    def df(self) -> pd.DataFrame:
        if self._df is None:
            self._load_df()
        return cast(pd.DataFrame, self._df)

    def __len__(self):
        return len(self.df)

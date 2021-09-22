import pandas as pd
from typing import List, Optional
from pandas.api.types import is_string_dtype

from ..io import isoparse


class QueryResult:
    def __init__(self, query: str, result):
        self.query = query
        self.result = result
        self._data = None
        self._df: Optional[pd.DataFrame] = None
        self._date_columns: List[str] = []

        # We fetch columns and data here, but do not store 'result'
        # since we might use the QueryResult in another thread.
        # self.columns = result.keys()
        # self.data = result.fetchall()

    @property
    def data(self):
        if self._data is None:
            self._data = self.result.fetchall()
        return self._data

    @property
    def columns(self) -> List[str]:
        return self.result.keys()

    @property
    def date_columns(self) -> List[str]:
        if self._df is None:
            self._load_df()
        return self._date_columns

    @property
    def string_columns(self) -> List[str]:
        if self._df is None:
            self._load_df()
        assert isinstance(self._df, pd.DataFrame)
        return [
            k
            for k in self._df.keys()
            if is_string_dtype(self._df[k]) and k not in self.date_columns
        ]

    def _isoparse(self, column):
        if len(column) < 10:
            # must be at least of format YYYY-MM-DD to be a date
            raise ValueError()
        return isoparse(column)

    def _parse_date_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        for column_name, column in df.items():
            try:
                _col = column.apply(isoparse)
                df[column_name] = _col
                self._date_columns.append(str(column_name))
            except Exception:
                pass
        return df

    def _load_df(self):
        # use coerce_float=True as it is used in pandas by default
        frame = pd.DataFrame.from_records(
            self.data, columns=self.columns, coerce_float=True
        )
        self._df = self._parse_date_columns(frame)

    @property
    def df(self) -> pd.DataFrame:
        if self._df is None:
            self._load_df()
        assert isinstance(self._df, pd.DataFrame)
        return self._df

    def __len__(self):
        return len(self.df)

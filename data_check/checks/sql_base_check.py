from pathlib import Path
from typing import Union, List, cast, Tuple
import pandas as pd
import numpy as np

from .base_check import BaseCheck
from data_check.sql.query_result import QueryResult
from ..result import DataCheckResult, ResultType
from ..io import read_sql_file


class SQLBaseCheck(BaseCheck):
    """Implements basic functionality for SQL checks. Base class for others, not really a check in itself."""

    def get_sql_result(self) -> Union[DataCheckResult, QueryResult]:
        return self.read_sql_file(sql_file=self.check_path)

    def read_sql_file(self, sql_file: Path) -> Union[DataCheckResult, QueryResult]:
        try:
            query = read_sql_file(
                sql_file=sql_file, template_data=self.data_check.template_data
            )
            return self.data_check.sql.run_query_with_result(
                query, params=self.data_check.sql_params
            )
        except Exception as exc:
            return self.data_check.output.prepare_result(
                ResultType.FAILED_WITH_EXCEPTION, source=sql_file, exception=exc
            )

    @staticmethod
    def merge_results(
        sql_result: pd.DataFrame, expect_result: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Merges the results of a SQL query and the expected results.
        Returns the merged DataFrame.
        """
        SQLBaseCheck.convert_mixed_object_columns(sql_result, expect_result)

        try:
            df_merged = sql_result.merge(expect_result, indicator=True, how="outer")
        except ValueError:
            # treat both columns as str if their data types differ
            for sc in cast(List[str], sql_result.columns):
                if sc in expect_result.columns:
                    if sql_result[sc].dtype != expect_result[sc].dtype:
                        sql_result[sc], expect_result[sc] = SQLBaseCheck.convert_dtypes(
                            sql_result[sc], expect_result[sc]
                        )
            df_merged = sql_result.merge(expect_result, indicator=True, how="outer")
        return df_merged

    @staticmethod
    def convert_mixed_object_columns(df_1: pd.DataFrame, df_2: pd.DataFrame):
        # If we have object columns, convert them to string
        # if they contain mixed int/float and string values.
        object_columns = set(df_1.columns[df_1.dtypes == "object"])
        object_columns.update(set(df_2.columns[df_2.dtypes == "object"]))
        for o_col in object_columns:
            if o_col in df_1.columns and o_col in df_2.columns:
                df_1_types = set(type(el) for el in df_1[o_col].array)
                df_2_types = set(type(el) for el in df_2[o_col].array)
                both_types = df_1_types.union(df_2_types)
                if both_types in (
                    set([str, int]),
                    set([str, float]),
                    set([str, int, float]),
                ):
                    # convert only if str is mixed with a numeric type
                    df_1[o_col], df_2[o_col] = SQLBaseCheck.convert_dtypes(
                        df_1[o_col], df_2[o_col]
                    )

    def cleanup(self):
        self.data_check.sql.disconnect()

    @staticmethod
    def convert_dtypes(
        col_1: pd.Series, col_2: pd.Series
    ) -> Tuple[pd.Series, pd.Series]:
        # float64 can be in scientific notation, so it cannot be compared against a str
        if col_1.dtype == np.float64 or col_2.dtype == np.float64:
            try:
                col_1 = col_1.astype(np.float64)
            except:
                pass
            try:
                col_2 = col_2.astype(np.float64)
            except:
                pass
        else:
            col_1 = col_1.astype("str")
            col_2 = col_2.astype("str")
        return col_1, col_2

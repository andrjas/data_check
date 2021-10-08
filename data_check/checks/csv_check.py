from pathlib import Path
import pandas as pd
from typing import Tuple, Union

from data_check.sql.query_result import QueryResult

from .base_check import BaseCheck
from ..result import DataCheckResult, ResultType
from ..io import read_sql_file, get_expect_file, read_csv


class CSVCheck(BaseCheck):
    @staticmethod
    def merge_results(
        sql_result: pd.DataFrame, expect_result: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Merges the results of a SQL query and the expected results.
        Returns the merged DataFrame.
        """
        try:
            df_merged = sql_result.merge(expect_result, indicator=True, how="outer")
        except ValueError:
            # treat both columns as str if their data types differ
            for sc in sql_result.columns:
                if sc in expect_result.columns:
                    if sql_result[sc].dtype != expect_result[sc].dtype:
                        expect_result[sc] = expect_result[sc].astype("str")
                        sql_result[sc] = sql_result[sc].astype("str")
            df_merged = sql_result.merge(expect_result, indicator=True, how="outer")
        return df_merged

    @staticmethod
    def is_check_path(path: Path):
        return path.suffix.lower() == ".sql"

    @staticmethod
    def get_result(
        sql_result: pd.DataFrame, expect_result: pd.DataFrame, return_all: bool
    ) -> Tuple[ResultType, pd.DataFrame]:
        # replace missing values and None with pd.NA
        sql_result.fillna(value=pd.NA, inplace=True)
        # using "" instead of r'^$' doesn't work somehow, so we need to use regex
        sql_result.replace(r"^$", pd.NA, regex=True, inplace=True)

        expect_result.fillna(value=pd.NA, inplace=True)

        df_merged = CSVCheck.merge_results(sql_result, expect_result)
        df_diff = df_merged[df_merged._merge != "both"]
        assert isinstance(df_diff, pd.DataFrame)  # assert a DataFrame, not a Series
        df_result = df_merged if return_all else df_diff

        # empty diff means there are no differences and the test has passed
        passed = len(df_diff) == 0
        if passed and len(sql_result) != len(expect_result):
            return (ResultType.FAILED_DIFFERENT_LENGTH, df_result)
        return (DataCheckResult.passed_to_result_type(passed), df_result)

    def read_sql_file(self, sql_file: Path) -> Union[DataCheckResult, QueryResult]:
        try:
            query = read_sql_file(
                sql_file=sql_file, template_data=self.data_check.template_data
            )
            return self.data_check.sql.run_query_with_result(query)
        except Exception as exc:
            return self.data_check.output.prepare_result(
                ResultType.FAILED_WITH_EXCEPTION, source=sql_file, exception=exc
            )

    def read_expect_file(
        self, expect_file: Path, parse_dates, string_columns
    ) -> Union[DataCheckResult, pd.DataFrame]:
        try:
            expect_result = read_csv(
                expect_file,
                parse_dates=parse_dates,
                string_columns=string_columns,
            )
            return expect_result
        except Exception as exc_csv:
            return self.data_check.output.prepare_result(
                ResultType.FAILED_WITH_EXCEPTION, source=expect_file, exception=exc_csv
            )

    def get_expect_file(self, sql_file: Path) -> Path:
        return get_expect_file(sql_file)

    def run_test(
        self,
        return_all: bool = False,
    ) -> DataCheckResult:
        """
        Run a data_check test on a single input file.
        Returns a DataCheckResult with the result.

        If return_all is set, the DataCheckResult will contail all results,
        not only the failed ones.
        """
        sql_file = self.check_path
        expect_file = self.get_expect_file(sql_file)
        if not expect_file.exists():
            # no need to run queries, if no expected results found
            return self.data_check.output.prepare_result(
                ResultType.NO_EXPECTED_RESULTS_FILE, source=sql_file
            )
        sql_result = self.read_sql_file(sql_file=sql_file)
        if isinstance(sql_result, DataCheckResult):
            return sql_result

        expect_result = self.read_expect_file(
            expect_file, sql_result.date_columns, sql_result.string_columns
        )
        if isinstance(expect_result, DataCheckResult):
            return expect_result

        result_type, df_result = CSVCheck.get_result(
            sql_result.df, expect_result, return_all
        )
        return self.data_check.output.prepare_result(
            result_type, source=sql_file, result=df_result
        )

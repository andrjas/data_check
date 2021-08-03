from pathlib import Path
import pandas as pd
from typing import List, Tuple, Dict, Any

from ..config import DataCheckConfig
from ..result import DataCheckResult, ResultType
from ..output import DataCheckOutput
from ..io import expand_files, read_sql_file, get_expect_file, read_csv, read_yaml


class SimpleCheck:
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
    def is_simple_check(path: Path):
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

        df_merged = SimpleCheck.merge_results(sql_result, expect_result)
        df_diff = df_merged[df_merged._merge != "both"]
        assert isinstance(df_diff, pd.DataFrame)  # assert a DataFrame, not a Series
        df_result = df_merged if return_all else df_diff

        # empty diff means there are no differences and the test has passed
        passed = len(df_diff) == 0
        return (DataCheckResult.passed_to_result_type(passed), df_result)

    def get_date_columns(self, df: pd.DataFrame) -> List[str]:
        return [k for k in df.keys() if pd.api.types.is_datetime64_any_dtype(df[k])]

    def get_string_columns(self, df: pd.DataFrame) -> List[str]:
        return [k for k in df.keys() if pd.api.types.is_string_dtype(df[k])]

    def run_test(
        self,
        sql_file: Path,
        return_all: bool = False,
    ) -> DataCheckResult:
        """
        Run a data_check test on a single input file.
        Returns a DataCheckResult with the result.

        If return_all is set, the DataCheckResult will contail all results,
        not only the failed ones.
        """
        expect_file = get_expect_file(sql_file)
        if not expect_file.exists():
            # no need to run queries, if no expected results found
            return self.output.prepare_result(
                ResultType.NO_EXPECTED_RESULTS_FILE, source=sql_file
            )

        try:
            query = read_sql_file(sql_file=sql_file, template_data=self.template_data)
            sql_result = self.sql.run_query(query)
            date_columns = self.get_date_columns(sql_result)
            string_columns = self.get_string_columns(sql_result)
            extra_dates = self.sql.parse_date_hint(query)
            for ed in extra_dates:
                sql_result[ed] = sql_result[ed].apply(self.sql.date_parser)
        except Exception as exc:
            return self.output.prepare_result(
                ResultType.FAILED_WITH_EXCEPTION, source=sql_file, exception=exc
            )

        try:
            expect_result = read_csv(
                expect_file, parse_dates=date_columns, string_columns=string_columns
            )
            for ed in extra_dates:
                expect_result[ed] = expect_result[ed].apply(self.sql.date_parser)
        except Exception as exc_csv:
            return self.output.prepare_result(
                ResultType.FAILED_WITH_EXCEPTION, source=expect_file, exception=exc_csv
            )

        result_type, df_result = SimpleCheck.get_result(
            sql_result, expect_result, return_all
        )
        return self.output.prepare_result(
            result_type, source=sql_file, result=df_result
        )

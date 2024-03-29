from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Union, cast

import pandas as pd

from ..file_ops import get_expect_file, read_csv
from ..result import DataCheckResult, ResultType
from .sql_base_check import SQLBaseCheck

if TYPE_CHECKING:
    from data_check import DataCheck


@dataclass
class CSVCheckResult:
    result_type: ResultType
    diff: Optional[pd.DataFrame] = None
    merged: Optional[pd.DataFrame] = None
    exception: Optional[Exception] = None


class CSVCheck(SQLBaseCheck):
    def __init__(self, data_check: DataCheck, check_path: Path) -> None:
        if check_path.suffix.lower() in (".csv", ".xlsx"):
            check_path = check_path.with_suffix(".sql")
        super().__init__(data_check, check_path)

    @staticmethod
    def is_check_path(path: Path):
        return path.suffix.lower() == ".sql" or (
            path.suffix.lower() in (".csv", ".xlsx")
            and path.with_suffix(".sql").exists()
        )

    def get_result(
        self, sql_result: pd.DataFrame, expect_result: pd.DataFrame
    ) -> CSVCheckResult:
        # replace missing values and None with pd.NA
        sql_result.fillna(value=pd.NA, inplace=True)
        # using "" instead of r'^$' doesn't work somehow, so we need to use regex
        sql_result.replace(r"^$", pd.NA, regex=True, inplace=True)
        # replace empty datetime values with empty string
        sql_result.replace({pd.NaT: pd.NA}, inplace=True)

        expect_result.fillna(value=pd.NA, inplace=True)
        expect_result.replace(r"^$", pd.NA, regex=True, inplace=True)
        expect_result.replace({pd.NaT: pd.NA}, inplace=True)

        try:
            df_merged = SQLBaseCheck.merge_results(sql_result, expect_result)
        except Exception as e:
            return CSVCheckResult(ResultType.FAILED_WITH_EXCEPTION, exception=e)
        df_diff = cast(pd.DataFrame, df_merged[df_merged._merge != "both"])

        # empty diff means there are no differences and the test has passed
        passed = len(df_diff) == 0
        if passed and len(sql_result) != len(expect_result):
            return CSVCheckResult(
                ResultType.FAILED_DIFFERENT_LENGTH, df_diff, df_merged
            )
        return CSVCheckResult(
            DataCheckResult.passed_to_result_type(passed), df_diff, df_merged
        )

    def read_expect_file(
        self, expect_file: Path, string_columns: list[str]
    ) -> Union[DataCheckResult, pd.DataFrame]:
        try:
            expect_result = read_csv(
                expect_file,
                string_columns=string_columns,
            )
            return expect_result
        except Exception as exc_csv:
            return self.data_check.output.prepare_result(
                ResultType.FAILED_WITH_EXCEPTION, source=expect_file, exception=exc_csv
            )

    def get_expect_file(self, sql_file: Path) -> Path:
        return get_expect_file(sql_file)

    def run_test(self) -> DataCheckResult:
        """
        Run a data_check test on a single input file.
        Returns a DataCheckResult with the result.
        """
        sql_file = self.check_path
        expect_file = self.get_expect_file(sql_file)
        if not expect_file.exists():
            # no need to run queries, if no expected results found
            return self.data_check.output.prepare_result(
                ResultType.NO_EXPECTED_RESULTS_FILE, source=sql_file
            )
        sql_result = self.get_sql_result()
        if isinstance(sql_result, DataCheckResult):
            return sql_result

        expect_result = self.read_expect_file(expect_file, sql_result.string_columns)
        if isinstance(expect_result, DataCheckResult):
            return expect_result

        result = self.get_result(sql_result.df, expect_result)
        return self.data_check.output.prepare_result(
            result.result_type,
            source=sql_file,
            result=result.diff,
            full_result=result.merged,
            exception=result.exception,
        )

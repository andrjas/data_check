from pathlib import Path
import pandas as pd
from colorama import Fore, Style
import traceback
from os import linesep
from typing import Optional

from .exceptions import DataCheckException
from .result import DataCheckResult, ResultType


class DataCheckOutput:
    def __init__(self):
        self.verbose = False
        self.traceback = False
        self.print_failed = False
        self.print_format = "pandas"

    def configure_output(
        self, verbose: bool, traceback: bool, print_failed: bool, print_format: str
    ):
        self.verbose = verbose
        self.traceback = traceback
        self.print_failed = print_failed
        self.print_format = print_format

    def pprint_overall_result(self, passed: bool) -> None:
        overall_result_msg = self.passed_message if passed else self.failed_message
        print(
            ""
        )  # print newline to separate other results from the overall result message
        print(f"overall result: {overall_result_msg}")

    def pprint_failed(self, df: pd.DataFrame) -> str:
        """
        Prints a DataFrame with diff information and returns it as a string.
        """
        df["_diff"] = ""
        df.loc[df._merge == "left_only", ["_diff"]] = "db"
        df.loc[df._merge == "right_only", ["_diff"]] = "expected"
        return self.pprint_df(df.drop(["_merge"], axis=1))

    def pprint_df(self, df: pd.DataFrame) -> str:
        with pd.option_context("display.max_rows", None, "display.max_columns", None):
            if self.print_format == "pandas":
                return str(df)
            elif self.print_format.lower() == "csv":
                return df.to_csv(index=False)
            elif self.print_format.lower() == "json":
                return df.to_json(orient="table", indent=2)
            else:
                raise DataCheckException(f"unknown print format: {self.print_format}")

    @staticmethod
    def str_pass(string: str) -> str:
        return Fore.GREEN + string + Style.RESET_ALL

    @staticmethod
    def str_warn(string: str) -> str:
        return Fore.YELLOW + string + Style.RESET_ALL

    @staticmethod
    def str_fail(string: str) -> str:
        return Fore.RED + string + Style.RESET_ALL

    @property
    def passed_message(self) -> str:
        return self.str_pass("PASSED")

    @property
    def failed_message(self) -> str:
        return self.str_fail("FAILED")

    def prepare_result(
        self,
        result_type: ResultType,
        source: Path,
        result: Optional[pd.DataFrame] = None,
        exception: Optional[Exception] = None,
    ) -> DataCheckResult:
        passed = DataCheckResult.result_type_passed(result_type)
        if result_type == ResultType.PASSED:
            assert isinstance(result, pd.DataFrame)
            return self._passed_result(passed, source, result)
        elif result_type == ResultType.FAILED:
            assert isinstance(result, pd.DataFrame)
            return self._failed_result(passed, source, result)
        elif result_type == ResultType.NO_EXPECTED_RESULTS_FILE:
            return self._no_expected_file_result(passed, source)
        elif result_type == ResultType.FAILED_WITH_EXCEPTION:
            assert isinstance(exception, Exception)
            return self._failed_with_exception_result(passed, source, exception)

    def _passed_result(
        self, passed: bool, source: Path, result: pd.DataFrame
    ) -> DataCheckResult:
        message = f"{source}: {self.passed_message}"
        return DataCheckResult(passed=passed, result=result, message=message)

    def _failed_result(
        self, passed: bool, source: Path, result: pd.DataFrame
    ) -> DataCheckResult:
        message = f"{source}: {self.failed_message}"
        if self.print_failed:
            message += linesep + self.pprint_failed(result.copy())
        return DataCheckResult(passed=passed, result=result, message=message)

    def _no_expected_file_result(self, passed: bool, source: Path) -> DataCheckResult:
        warn = self.str_warn("NO EXPECTED RESULTS FILE")
        message = f"{source}: {warn}"
        return DataCheckResult(
            passed=passed,
            result=f"{source}: NO EXPECTED RESULTS FILE",
            message=message,
        )

    def _failed_with_exception_result(
        self, passed: bool, source: Path, exception: Exception
    ):
        fail = self.str_fail(f"FAILED (with exception in {source})")
        message = f"{source}: {fail}"
        if self.verbose:
            message += linesep + str(exception)
        if self.traceback:
            message += linesep + traceback.format_exc()
        return DataCheckResult(
            passed=passed,
            result=f"{source} generated an exception: {exception}",
            message=message,
        )

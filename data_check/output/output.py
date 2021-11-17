from pathlib import Path
import pandas as pd
from colorama import Fore, Style
import traceback
from os import linesep
from typing import IO, Optional, Any, Tuple, Union, List, cast

from ..exceptions import DataCheckException
from ..result import DataCheckResult, ResultType
from ..io import rel_path
from .handler import OutputHandler


class DataCheckOutput:
    def __init__(self):
        self.verbose = False
        self.traceback = False
        self.print_failed = False
        self.print_format = "pandas"
        self.quiet = False
        self.handler = OutputHandler(self.quiet)

    def configure_output(
        self,
        verbose: bool,
        traceback: bool,
        print_failed: bool,
        print_format: str,
        quiet: bool = False,
    ):
        self.verbose = verbose
        self.traceback = traceback
        self.print_failed = print_failed
        self.print_format = print_format
        self.quiet = quiet

        self.handler.quiet = quiet

    def print(self, msg: Any, prefix: str = ""):
        self.handler.print(msg, prefix)

    def log(self, msg: Any, prefix: str = "", level: str = "INFO"):
        self.handler.log(msg, prefix, level)

    def handle_subprocess_output(self, pipe: IO[bytes]):
        self.handler.handle_subprocess_output(pipe)

    def pprint_overall_result(self, passed: bool) -> None:
        overall_result_msg = self.passed_message if passed else self.failed_message
        # print newline to separate other results from the overall result message
        self.print("")
        self.print(f"overall result: {overall_result_msg}")

    def prepare_pprint_df(self, df: pd.DataFrame) -> pd.DataFrame:
        if "_merge" in df.columns:
            df["_diff"] = ""
            df.loc[df._merge == "left_only", ["_diff"]] = "db"
            df.loc[df._merge == "right_only", ["_diff"]] = "expected"
            df = df.drop(["_merge"], axis=1)
        return df.sort_values(by=list(df.columns), axis=0)

    def pprint_failed(self, df: pd.DataFrame) -> str:
        """
        Prints a DataFrame with diff information and returns it as a string.
        """
        return self.pprint_df(self.prepare_pprint_df(df))

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
        result: Union[pd.DataFrame, List[Tuple[str, pd.DataFrame]], None] = None,
        exception: Optional[Exception] = None,
    ) -> DataCheckResult:
        passed = DataCheckResult.result_type_passed(result_type)
        # always print path relative to where data_check is started
        rel_source = rel_path(source)
        if result_type == ResultType.PASSED:
            return self._passed_result(passed, rel_source, cast(pd.DataFrame, result))
        elif result_type == ResultType.FAILED:
            return self._failed_result(passed, rel_source, cast(pd.DataFrame, result))
        elif result_type == ResultType.FAILED_WITH_MULTIPLE_FAILURES:
            return self._failed_result_multiple_failures(
                passed, rel_source, cast(List[Tuple[str, pd.DataFrame]], result)
            )
        elif result_type == ResultType.NO_EXPECTED_RESULTS_FILE:
            return self._no_expected_file_result(passed, rel_source)
        elif result_type == ResultType.FAILED_WITH_EXCEPTION:
            return self._failed_with_exception_result(
                passed, rel_source, cast(Exception, exception)
            )
        elif result_type == ResultType.FAILED_DIFFERENT_LENGTH:
            return self._failed_result_different_length(passed, rel_source)
        elif result_type == ResultType.FAILED_PATH_NOT_EXISTS:
            return self._failed_path_not_exists(passed, rel_source)
        else:
            raise Exception(f"unknown ResultType: {result_type}")

    def _passed_result(
        self, passed: bool, source: Path, result: pd.DataFrame
    ) -> DataCheckResult:
        message = f"{source}: {self.passed_message}"
        if self.print_failed and self.verbose:
            message += linesep + self.pprint_failed(result.copy())
        return DataCheckResult(passed=passed, result=result, message=message)

    def _failed_result(
        self, passed: bool, source: Path, result: pd.DataFrame
    ) -> DataCheckResult:
        message = f"{source}: {self.failed_message}"
        if self.print_failed:
            message += linesep + self.pprint_failed(result.copy())
        return DataCheckResult(passed=passed, result=result, message=message)

    def _failed_result_multiple_failures(
        self, passed: bool, source: Path, result: List[Tuple[str, pd.DataFrame]]
    ) -> DataCheckResult:
        message = f"{source}: {self.failed_message}"
        failure_message = ""
        for failure in result:
            failure_message += (
                linesep
                + failure[0]
                + ":"
                + linesep
                + self.pprint_failed(failure[1].copy())
            )
        if self.print_failed:
            message += failure_message
        return DataCheckResult(passed=passed, result=failure_message, message=message)

    def _failed_result_different_length(
        self, passed: bool, source: Path
    ) -> DataCheckResult:
        message = f"{source}: {self.failed_message}"
        result = "same data but the length differs"
        if self.print_failed:
            message += linesep + result
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
            message += linesep + "".join(
                traceback.format_exception(
                    value=exception, tb=exception.__traceback__, etype=Exception
                )
            )
        return DataCheckResult(
            passed=passed,
            result=f"{source} generated an exception: {exception}",
            message=message,
        )

    def _failed_path_not_exists(self, passed: bool, source: Path) -> DataCheckResult:
        warn = self.str_warn("PATH DOESN'T EXIST")
        message = f"{source}: {warn}"
        return DataCheckResult(
            passed=passed,
            result=f"{source}: PATH DOESN'T EXIST",
            message=message,
        )

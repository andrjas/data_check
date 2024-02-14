import sys
import traceback
from pathlib import Path
from typing import IO, Any, Callable, List, Optional, Tuple, Union

import pandas as pd
from colorama import Fore, Style

from ..exceptions import DataCheckException
from ..file_ops import rel_path
from ..result import DataCheckResult, ResultType
from .diffed_df import get_diffed_df
from .handler import OutputHandler
from .result_formatter import format_data_check_result


class DataCheckOutput:
    def __init__(self):
        self.verbose = False
        self.traceback = False
        self.print_failed = False
        self.print_format: str = "pandas"
        self.print_diffed = False
        self.quiet = False
        self.handler = OutputHandler(self.quiet)
        self.log_path: Optional[Path] = None

    def configure_output(
        self,
        verbose: bool,
        traceback: bool,
        print_failed: bool = False,
        print_format: str = "pandas",
        print_diffed: bool = False,
        quiet: bool = False,
        log_path: Optional[Path] = None,
        printer: Optional[Callable[[Optional[Any]], None]] = None,
    ):
        self.verbose = verbose
        self.traceback = traceback
        self.print_failed = print_failed
        self.print_format = print_format
        self.print_diffed = print_diffed
        self.quiet = quiet
        self.log_path = log_path

        self.handler.quiet = quiet
        self.handler.log_path = log_path
        if printer:
            self.handler.printer = printer

    def configure_print(
        self,
        print_failed: Optional[bool] = None,
        print_format: Optional[str] = None,
        print_diffed: Optional[bool] = None,
    ):
        if print_failed is not None:
            self.print_failed = print_failed
        if print_format is not None:
            self.print_format = print_format
        if print_diffed is not None:
            self.print_diffed = print_diffed

    @staticmethod
    def format_exception(exc: Exception):
        if sys.version_info >= (3, 10):
            return traceback.format_exception(exc)
        return traceback.format_exception(
            value=exc, tb=exc.__traceback__, etype=Exception
        )

    def print_exception(self, exc: Exception):
        if self.traceback:
            self.print("".join(self.format_exception(exc)))
        else:
            self.print(str(exc))

    def print(self, msg: Any):
        if isinstance(msg, DataCheckResult):
            msg.prepare_message(self.prepare_data_check_result)
            self.handler.print(msg.message, log_msg=msg.log_message)
        elif isinstance(msg, pd.DataFrame):
            self.handler.print(self.pprint_df(msg))
        else:
            self.handler.print(msg)

    def prepare_data_check_result(self, result: DataCheckResult):
        format_data_check_result(self, result)

    def handle_subprocess_output(self, pipe: IO[bytes], _print: bool = True):
        self.handler.handle_subprocess_output(pipe, _print=_print)

    def pprint_overall_result(self, passed: bool) -> None:
        overall_result_msg = self.passed_message if passed else self.failed_message
        # print newline to separate other results from the overall result message
        self.print("")
        self.print(f"overall result: {overall_result_msg}")

    def pprint_result_summary(self, results: List[DataCheckResult]) -> None:
        self.print("")
        passed = len([r for r in results if r.passed])
        warnings = len([r for r in results if r.is_warning])
        failed = len([r for r in results if not r.passed]) - warnings
        self.print(
            f"summary: {passed} {self.str_pass('passed')}, {failed} "
            f"{self.str_fail('failed')}, {warnings} {self.str_warn('warnings')}"
        )

    def _get_df(self, result: Union[DataCheckResult, pd.DataFrame]) -> pd.DataFrame:
        if isinstance(result, pd.DataFrame):
            return result
        else:
            if self.verbose and result.full_result is not None:
                return result.full_result
            elif isinstance(result.result, pd.DataFrame):
                return result.result
            return pd.DataFrame()

    def prepare_pprint_df(
        self, result: Union[DataCheckResult, pd.DataFrame]
    ) -> pd.DataFrame:
        df = self._get_df(result)
        if self.print_diffed and isinstance(result, DataCheckResult):
            df = get_diffed_df(df, result)
        if "_merge" in df.columns:
            df = df.copy()
            df["_diff"] = ""
            df.loc[df._merge == "left_only", ["_diff"]] = "db"
            df.loc[df._merge == "right_only", ["_diff"]] = "expected"
            df.loc[df._merge == "both", ["_diff"]] = "same"
            df = df.drop(["_merge"], axis=1)
        return df.sort_values(by=list(df.columns), axis=0)

    def pprint_result(self, result: Union[DataCheckResult, pd.DataFrame]) -> str:
        """
        Prints a DataFrame with diff information and returns it as a string.
        """
        return self.pprint_df(self.prepare_pprint_df(result))

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
        full_result: Optional[pd.DataFrame] = None,
    ) -> DataCheckResult:
        passed = DataCheckResult.result_type_passed(result_type)
        # always print path relative to where data_check is started
        rel_source = rel_path(source)
        source_message = ""
        extra_message = ""
        d_result = "" if result is None else result

        if result_type == ResultType.NO_EXPECTED_RESULTS_FILE:
            source_message = self.str_warn("NO EXPECTED RESULTS FILE")
        elif result_type == ResultType.FAILED_DIFFERENT_LENGTH:
            extra_message = "same data but the length differs"
        elif result_type == ResultType.FAILED_PATH_NOT_EXISTS:
            source_message = self.str_warn("PATH DOESN'T EXIST")

        return DataCheckResult(
            passed=passed,
            source=rel_source,
            result=d_result,
            exception=exception,
            source_message=source_message,
            extra_message=extra_message,
            full_result=full_result,
            result_type=result_type,
        )

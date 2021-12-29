from __future__ import annotations
from pathlib import Path
import pandas as pd
from typing import Callable, Optional, Union, List, Tuple
from enum import Enum
from dataclasses import dataclass


class ResultType(Enum):
    PASSED = 1
    FAILED = 2
    FAILED_WITH_EXCEPTION = 3
    NO_EXPECTED_RESULTS_FILE = 4
    FAILED_DIFFERENT_LENGTH = 5
    FAILED_PATH_NOT_EXISTS = 6
    FAILED_WITH_MULTIPLE_FAILURES = 7


@dataclass
class DataCheckResult:
    """
    Wrapper class holding the result of a test run.
    """

    passed: bool
    """True, if the result is from a passed check, False if the check failed."""
    source: Union[Path, str]
    """Source of the result."""
    result: Union[pd.DataFrame, str, List[Tuple[str, pd.DataFrame]]] = ""
    """Additional information for the result.
    If it is a DataFrame or a list with DataFrames, the DataFrame contains the non-matching results.
    If is is a string, the string is used for DataCheckResult.message.
    """
    _message: str = ""
    """Content for DataCheckResult.message.
    Must be prepared first with DataCheckResult.prepare_message().
    """
    exception: Optional[Exception] = None
    """The exception, if the check has thrown any."""
    source_message: str = ""
    """Additional information for the source."""
    extra_message: str = ""
    """Additional message for the result."""
    _log_message: str = ""
    """Content for the log message. Uses DataCheckResult.message if not explicitly set."""
    full_result: Optional[pd.DataFrame] = None
    """Optional DataFrame with full result information (pre-diff)"""

    def __bool__(self):
        return self.passed

    def __str__(self):
        return (
            f"DataCheckResult(passed={self.passed}, "
            f"result={self.result}, message={self.message})"
        )

    @property
    def message(self) -> str:
        """Message used for printing the result.
        Must be prepared first with DataCheckResult.prepare_message()."""
        if self._message:
            return self._message
        if isinstance(self.result, str):
            self._message = self.result
        return self._message

    @message.setter
    def message(self, msg: str):
        self._message = msg

    @property
    def log_message(self) -> str:
        """Message used for logging.
        Uses DataCheckResult.message if not explicitly set."""
        if self._log_message:
            return self._log_message
        else:
            return self.message

    @log_message.setter
    def log_message(self, msg: str):
        self._log_message = msg

    def prepare_message(self, prepare_method: Callable[[DataCheckResult], None]):
        """prepare_method is expected for format DataCheckResult.message
        and optionally DataCheckResult.log_message so it can be printed."""
        prepare_method(self)

    @staticmethod
    def result_type_passed(result_type: ResultType) -> bool:
        return result_type == ResultType.PASSED

    @staticmethod
    def passed_to_result_type(passed: bool) -> ResultType:
        if passed:
            return ResultType.PASSED
        else:
            return ResultType.FAILED

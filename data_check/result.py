import pandas as pd
from typing import Union
from enum import Enum


class ResultType(Enum):
    PASSED = 1
    FAILED = 2
    FAILED_WITH_EXCEPTION = 3
    NO_EXPECTED_RESULTS_FILE = 4


class DataCheckResult:
    """
    Wrapper class holding the result of a test run.
    """

    def __init__(
        self, passed: bool, result: Union[pd.DataFrame, str], message: str = ""
    ):
        self.passed = passed
        self.result = result
        self.message = message

    def __bool__(self):
        return self.passed

    def __str__(self):
        return (
            f"DataCheckResult(passed={self.passed}, "
            f"result={self.result}, message={self.message})"
        )

    @staticmethod
    def result_type_passed(result_type: ResultType) -> bool:
        return result_type == ResultType.PASSED

    @staticmethod
    def passed_to_result_type(passed: bool) -> ResultType:
        if passed:
            return ResultType.PASSED
        else:
            return ResultType.FAILED

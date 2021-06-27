import pandas as pd
from typing import Union


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

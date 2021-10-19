from pathlib import Path

from .base_check import BaseCheck
from ..result import DataCheckResult, ResultType


class PathNotExists(BaseCheck):
    """Special "check" that is used when a path/file that should contain a check doesn't exists.
    Always returns a failing result.
    """

    def run_test(self) -> DataCheckResult:
        return self.data_check.output.prepare_result(
            ResultType.FAILED_PATH_NOT_EXISTS, source=self.check_path
        )

    @staticmethod
    def is_check_path(path: Path) -> bool:
        return not path.exists()

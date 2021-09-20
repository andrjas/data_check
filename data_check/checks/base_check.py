from pathlib import Path

from ..result import DataCheckResult


class BaseCheck:
    def __init__(self, data_check, check_path: Path) -> None:
        self.data_check = data_check
        self.check_path = check_path  # path to the check (some file or folder)

    def run_test(self) -> DataCheckResult:
        pass

    @staticmethod
    def is_check_path(path: Path) -> bool:
        return False

    def __lt__(self, other) -> bool:
        return self.check_path < other.check_path

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} ({self.check_path})>"

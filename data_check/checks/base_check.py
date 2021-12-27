from __future__ import annotations
from pathlib import Path
from typing import TYPE_CHECKING

from ..result import DataCheckResult

if TYPE_CHECKING:
    from data_check import DataCheck


class BaseCheck:
    def __init__(self, data_check: DataCheck, check_path: Path) -> None:
        self.data_check = data_check
        self.check_path = check_path  # path to the check (some file or folder)

    def run_test(self) -> DataCheckResult:
        raise NotImplementedError()

    @staticmethod
    def is_check_path(path: Path) -> bool:
        return False

    def __lt__(self, other: BaseCheck) -> bool:
        return self.check_path < other.check_path

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} ({self.check_path})>"

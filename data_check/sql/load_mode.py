from enum import Enum

from ..exceptions import DataCheckException


class LoadMode(Enum):
    TRUNCATE = 1
    APPEND = 2
    REPLACE = 3

    @staticmethod
    def from_string(mode_name: str):
        if mode_name == "truncate":
            return LoadMode.TRUNCATE
        elif mode_name == "append":
            return LoadMode.APPEND
        elif mode_name == "replace":
            return LoadMode.REPLACE
        else:
            raise DataCheckException(f"unknown load mode: {mode_name}")

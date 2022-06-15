from enum import Enum

from ..exceptions import DataCheckException


class LoadMode(Enum):
    TRUNCATE = 1
    APPEND = 2
    REPLACE = 3
    UPSERT = 4

    @staticmethod
    def from_string(mode_name: str):
        mode = {
            "truncate": LoadMode.TRUNCATE,
            "append": LoadMode.APPEND,
            "replace": LoadMode.REPLACE,
            "upsert": LoadMode.UPSERT,
        }.get(mode_name.lower(), None)

        if not mode:
            raise DataCheckException(f"unknown load mode: {mode_name}")
        else:
            return mode

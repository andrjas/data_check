import multiprocessing
from typing import Any
import os


class OutputHandler:
    """
    Handles the output for data_check.
    """

    def __init__(self, quiet: bool):
        self.quiet = quiet

    @property
    def is_main(self):
        return multiprocessing.current_process().name == "MainProcess"

    def write_log(self, msg: str):
        pass

    def print(self, msg: Any, prefix: str = ""):
        if prefix:
            msg = f"{prefix}: {str(msg)}"
        else:
            msg = str(msg)
        if not self.quiet:
            print(msg)
        self.write_log(msg)

    def log(self, msg: Any, prefix: str = "", level="INFO"):
        if not self.is_main:
            prefix = f"{prefix}@{multiprocessing.current_process().name}"
        self.print(f"{level}: {str(msg)}", prefix=prefix)

    def handle_subprocess_output(self, pipe):
        for line in iter(pipe.readline, b""):
            self.print(line.decode().rstrip(os.linesep))

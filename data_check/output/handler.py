import multiprocessing
from typing import Any
import os
import locale


class OutputHandler:
    """
    Handles the output for data_check.
    """

    def __init__(self, quiet: bool):
        self.quiet = quiet
        self.encoding = locale.getpreferredencoding(False)

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
            try:
                # try printing the line with the system encoding
                self.print(line.decode(self.encoding).rstrip(os.linesep))
            except Exception:
                # if this doesn't help, print the raw bytes without the b''
                self.print(str(line)[2:-1].rstrip(os.linesep))

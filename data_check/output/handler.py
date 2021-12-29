from typing import IO, Any, Callable, Optional
import os
import locale
from pathlib import Path
import datetime


class OutputHandler:
    """
    Handles the output for data_check.
    """

    def __init__(self, quiet: bool):
        self.quiet = quiet
        self.encoding = locale.getpreferredencoding(False)
        self.log_path: Optional[Path] = None
        self.printer: Callable[[Optional[Any]], None] = print

    def write_log(self, msg: str):
        if self.log_path:
            with self.log_path.open("a") as log:
                s_log = f"{datetime.datetime.now()}: {msg}{os.linesep}"
                log.write(s_log)

    def print(self, msg: Any, _print: bool = True, log_msg: str = ""):
        msg = str(msg)
        if not self.quiet and _print:
            self.printer(msg)
        self.write_log(log_msg if log_msg else msg)

    def handle_subprocess_output(self, pipe: IO[bytes], print: bool = True):
        for line in iter(pipe.readline, b""):
            try:
                # try printing the line with the system encoding
                self.print(line.decode(self.encoding).rstrip(os.linesep), _print=print)
            except Exception:
                # if this doesn't help, print the raw bytes without the b''
                self.print(str(line)[2:-1].rstrip(os.linesep), _print=print)

from __future__ import annotations
from pathlib import Path
from typing import List, Union, TYPE_CHECKING
import subprocess

if TYPE_CHECKING:
    from data_check.output import DataCheckOutput


class CmdStep:
    def __init__(
        self, cmd: Union[str, List[str]], output: DataCheckOutput, print: bool = True
    ) -> None:
        self.cmd = cmd
        self.output = output
        self.print = print

    def _run_cmd(self, cmd: str, base_path: Path = Path(".")):
        self.output.print(f"# {cmd}")
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=base_path,
            shell=True,
        )
        assert process.stdout is not None
        with process.stdout:
            self.output.handle_subprocess_output(process.stdout, print=self.print)
        exitcode = process.wait()
        return exitcode == 0

    def run(self, base_path: Path = Path(".")):
        if isinstance(self.cmd, List):
            for c in self.cmd:
                if not self._run_cmd(c, base_path=base_path):
                    return False
        elif isinstance(self.cmd, str):
            return self._run_cmd(self.cmd, base_path=base_path)
        else:
            raise Exception(f"unknown cmd type: {self.cmd}")
        return True

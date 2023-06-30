import subprocess
from typing import List, Union

from pydantic import BaseModel, root_validator, validator

from data_check.config import DataCheckConfig
from data_check.output import DataCheckOutput

from .step import Step


class CmdStep(Step):
    commands: Union[str, List[str]]
    print: bool = True

    @validator("commands")
    def cmd_to_list(cls, v):
        if isinstance(v, str):
            return [v]
        elif isinstance(v, list):
            return v
        else:
            raise ValueError(f"unsupported cmd type: {v}")

    @property
    def output(self) -> DataCheckOutput:
        return self.data_check.output

    def run(self):
        for cmd in self.commands:
            if not self._run_cmd(cmd):
                return False
        return True

    def _run_cmd(self, cmd: str):
        self.output.print(f"# {cmd}")
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=self.base_path,
            shell=True,
        )
        assert process.stdout is not None
        with process.stdout:
            self.output.handle_subprocess_output(process.stdout, print=self.print)
        exitcode = process.wait()
        return exitcode == 0

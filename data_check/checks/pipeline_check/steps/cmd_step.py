import subprocess
from pathlib import Path
from typing import Union

from pydantic import field_validator

from data_check.output import DataCheckOutput

from ..pipeline_check import PipelineCheck
from .step import Step


class CmdStep(Step):
    commands: Union[str, list[str]]
    print: bool = True

    @field_validator("commands")
    @classmethod
    def cmd_to_list(cls, v):
        if isinstance(v, str):
            return [v]
        elif isinstance(v, list):
            return v
        else:
            raise ValueError(f"unsupported cmd type: {v}")

    def run(self, pipeline_check: PipelineCheck):
        for cmd in self.commands:
            if not self._run_cmd(
                cmd, pipeline_check.data_check.output, self.base_path(pipeline_check)
            ):
                return False
        return True

    def _run_cmd(self, cmd: str, output: DataCheckOutput, base_path: Path):
        output.print(f"# {cmd}")
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=base_path,
            shell=True,
        )
        assert process.stdout is not None
        with process.stdout:
            output.handle_subprocess_output(process.stdout, _print=self.print)
        exitcode = process.wait()
        return exitcode == 0

from pathlib import Path
from typing import Union

from pydantic import field_validator

from ..pipeline_check import PipelineCheck
from .step import Step, StrOrPathList


class FakeStep(Step):
    configs: StrOrPathList
    output: Union[str, Path] = Path()
    force: bool = False

    @field_validator("configs")
    @classmethod
    def configs_to_path_list(cls, v) -> list[Path]:
        return Step.to_path_list(v)

    @field_validator("output")
    @classmethod
    def output_to_path(cls, v) -> Path:
        return Path(v)

    def run(self, pipeline_check: PipelineCheck):
        return pipeline_check.data_check.fake_data(
            configs=Step.as_path_list(self.configs),
            output=Step.as_path(self.output),
            base_path=self.base_path(pipeline_check),
            force=self.force,
        )

    def validate_step(self, pipeline_check: PipelineCheck) -> bool:
        return Step.validate_path_list_exists(
            self.as_path_list(self.configs), self.base_path(pipeline_check)
        )

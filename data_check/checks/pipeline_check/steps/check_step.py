from pydantic import field_validator

from data_check.checks.pipeline_check.pipeline_check import PipelineCheck

from ..pipeline_check import PipelineCheck
from .step import Step, StrOrPathList


class CheckStep(Step):
    files: StrOrPathList

    def run(self, pipeline_check: PipelineCheck):
        return pipeline_check.data_check.run(
            files=Step.as_path_list(self.files),
            base_path=self.base_path(pipeline_check),
        )

    @field_validator("files")
    @classmethod
    def files_to_list(cls, v):
        return cls.to_path_list(v)

    def validate_step(self, pipeline_check: PipelineCheck) -> bool:
        return Step.validate_path_list_exists(
            self.as_path_list(self.files), self.base_path(pipeline_check)
        )

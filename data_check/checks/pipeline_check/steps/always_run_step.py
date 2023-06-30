from typing import Any, List

from pydantic import BaseModel, root_validator, validator

from .step import Step


class AlwaysRunStep(Step):
    steps: List[Step]

    @root_validator(pre=True)
    def always_run_steps(cls, values):
        from ..pipeline_model import PipelineModel

        pipeline_check = values["pipeline_check"]
        values["steps"] = [
            PipelineModel.concrete_step(pipeline_check, step)
            for step in values["steps"]
        ]
        return values

    def run(self):
        from ..pipeline_model import PipelineModel

        return PipelineModel.run_steps(self.steps, self.pipeline_check)

from collections.abc import Iterator

from pydantic import model_validator

from ..pipeline_check import PipelineCheck
from .step import Step


class AlwaysRunStep(Step):
    steps: list[Step]

    @model_validator(mode="before")
    @classmethod
    def always_run_steps(cls, values):
        from ..pipeline_model import concrete_step

        values["steps"] = [concrete_step(step) for step in values["steps"]]
        return values

    def run(self, pipeline_check: PipelineCheck):
        from ..pipeline_model import PipelineModel

        return PipelineModel.run_steps(self.steps_iterator, pipeline_check)

    @property
    def steps_iterator(self) -> Iterator[Step]:
        for step in self.steps:
            if not step.has_run:
                yield step

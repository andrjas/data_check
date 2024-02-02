from typing import Any, Optional

from ..pipeline_check import PipelineCheck
from .step import Step


class BreakpointStep(Step):
    args: Optional[Any] = None
    result: bool = True

    _pipeline_check: PipelineCheck

    def run(self, pipeline_check: PipelineCheck):
        self._pipeline_check = pipeline_check
        # set local variables to use in breakpoint
        data_check = pipeline_check.data_check  # noqa F401
        pipeline = pipeline_check  # noqa F401
        steps = self._pipeline_check.pipeline_model.steps  # noqa F401
        current_step = self.get_current_step  # noqa F401
        next_step = self.get_next_step  # noqa F401
        sql = pipeline_check.data_check.sql  # noqa F401
        run_next = self.run_next_step  # noqa F401

        breakpoint()
        return self.result

    def get_current_step(self):
        return self._pipeline_check.pipeline_model.current_step

    def get_next_step(self):
        return self._pipeline_check.pipeline_model.next_step

    def run_next_step(self):
        """
        Executes the next step of the pipeline and sets result to False
        if the step fails so that the whole pipeline fails.
        """
        try:
            result = self._pipeline_check.pipeline_model.run_next_step()
        except Exception:
            result = False
        if not result:
            self.result = result

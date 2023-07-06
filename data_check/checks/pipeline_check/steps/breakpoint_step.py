from typing import Any, Dict, Optional

from data_check.sql.load_mode import LoadMode

from .step import Step


class BreakpointStep(Step):
    args: Optional[Any] = None
    result: bool = True

    def run(self):
        # set local variables to use in breakpoint
        data_check = self.data_check  # noqa F401
        pipeline = self.pipeline_check  # noqa F401
        steps = self.pipeline_check.pipeline_model.steps  # noqa F401
        current_step = self.get_current_step  # noqa F401
        next_step = self.get_next_step  # noqa F401
        sql = self.data_check.sql
        run_next = self.run_next_step  # noqa F401

        breakpoint()
        return self.result

    def get_current_step(self):
        return self.pipeline_check.pipeline_model.current_step

    def get_next_step(self):
        return self.pipeline_check.pipeline_model.next_step

    def run_next_step(self):
        """
        Executes the next step of the pipeline and sets result to False if the step fails so that the whole pipeline fails.
        """
        try:
            result = self.pipeline_check.pipeline_model.run_next_step()
        except:
            result = False
        if not result:
            self.result = result

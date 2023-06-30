from typing import Any, Dict, Optional

from data_check.sql.load_mode import LoadMode

from .step import Step


class BreakpointStep(Step):
    kws: Optional[Dict[str, Any]] = None

    def run(self):
        # set local variables to use in breakpoint
        data_check = self.data_check
        pipeline = self.pipeline_check
        steps = self.pipeline_check.pipeline_model.steps
        sql = self.data_check.sql
        run_sql = sql.run_sql
        run_query = sql.run_query

        if self.kws:
            breakpoint(**self.kws)
        else:
            breakpoint()
        return True

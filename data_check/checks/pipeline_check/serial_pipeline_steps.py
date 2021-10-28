from pathlib import Path
from typing import List, Dict, Any
import inspect

from ...result import DataCheckResult, ResultType


class SerialPipelineSteps:
    def __init__(
        self,
        data_check,
        pipeline_check,
        steps: List[Any],
        path: Path,
        pipeline_name: str,
    ) -> None:
        self.data_check = data_check
        self.pipeline_check = pipeline_check
        self.path = path
        self.steps = steps
        self.pipeline_name = pipeline_name

    def run(self) -> DataCheckResult:
        for step in self.steps:
            try:
                result = self.run_pipeline_step(self.path, step)
                if not result:
                    result_msg = (
                        f"pipeline {self.pipeline_name}: "
                        f"{self.data_check.output.failed_message}"
                    )
                    return DataCheckResult(
                        passed=False,
                        result=result,
                        message=result_msg,
                    )
            except Exception as e:
                return self.data_check.output.prepare_result(
                    result_type=ResultType.FAILED_WITH_EXCEPTION,
                    source=self.path,
                    exception=e,
                )

        return DataCheckResult(
            passed=True,
            result=(
                f"pipeline {self.pipeline_name}: "
                f"{self.data_check.output.passed_message}"
            ),
        )

    def run_pipeline_step(self, path: Path, step: Dict[str, Any]):
        step_type = next(iter(step.keys()))
        params = next(iter(step.values()))
        call_method = self.pipeline_check.get_pipeline_method(step_type)
        if call_method:
            prepared_params = self.pipeline_check.get_prepared_parameters(
                step_type, params
            )
            argspec = inspect.getfullargspec(call_method)
            if "base_path" in argspec.args:
                prepared_params.update({"base_path": path})
            return call_method(**prepared_params)
        else:
            raise Exception(f"unknown pipeline step: {step_type}")

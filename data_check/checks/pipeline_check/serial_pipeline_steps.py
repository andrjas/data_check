from __future__ import annotations
from pathlib import Path
from typing import List, Dict, Any, TYPE_CHECKING, Union
import inspect

from ...result import DataCheckResult, ResultType

if TYPE_CHECKING:
    from data_check import DataCheck
    from data_check.checks import PipelineCheck


class SerialPipelineSteps:
    def __init__(
        self,
        data_check: DataCheck,
        pipeline_check: PipelineCheck,
        steps: List[Any],
        path: Path,
        pipeline_name: str,
    ) -> None:
        self.data_check = data_check
        self.pipeline_check = pipeline_check
        self.path = path
        self.steps = steps
        self.pipeline_name = pipeline_name
        self.processed_step_indices: List[int] = []

    def run(self) -> DataCheckResult:
        for idx, step in enumerate(self.steps):
            try:
                result = self.run_pipeline_step(self.path, step, idx)
                if not result:
                    self.process_always_run()
                    return DataCheckResult(
                        passed=False,
                        result=result,
                        source=f"pipeline {self.pipeline_name}",
                    )
            except Exception as e:
                self.process_always_run()
                return self.data_check.output.prepare_result(
                    result_type=ResultType.FAILED_WITH_EXCEPTION,
                    source=self.path,
                    exception=e,
                )

        return DataCheckResult(
            passed=True,
            source=f"pipeline {self.pipeline_name}",
        )

    @staticmethod
    def is_always_run_step(step: Union[Any, Dict[str, Any]]) -> bool:
        if isinstance(step, dict):
            return next(iter(step.keys()), None) == "always_run"
        return False

    def is_unprocessed_step(self, idx: int) -> bool:
        return idx not in self.processed_step_indices

    def process_always_run(self):
        for idx, step in enumerate(self.steps):
            if self.is_always_run_step(step) and self.is_unprocessed_step(idx):
                # just run the step but ignore any result or output
                self.run_pipeline_step(self.path, step, idx)

    def run_pipeline_step(self, path: Path, step: Dict[str, Any], idx: int):
        self.processed_step_indices.append(idx)
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

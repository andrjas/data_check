from __future__ import annotations

from contextlib import suppress
from typing import Any, Dict, Iterable, Iterator, List, Optional, Type

from pydantic import BaseModel, BeforeValidator, ConfigDict, model_validator
from typing_extensions import Annotated

from data_check.result import DataCheckResult, ResultType

from .pipeline_check import PipelineCheck
from .steps import (
    AlwaysRunStep,
    AppendStep,
    BreakpointStep,
    CheckStep,
    CmdStep,
    DeprecatedLoadTableStep,
    DeprecatedSqlFilesStep,
    FakeStep,
    LoadStep,
    PingStep,
    RunStep,
    SqlStep,
    Step,
)

STEP_TO_CLASS: Dict[str, Type[Step]] = {
    "load": LoadStep,
    "load_table": DeprecatedLoadTableStep,
    "check": CheckStep,
    "run": RunStep,
    "sql": SqlStep,
    "sql_files": DeprecatedSqlFilesStep,
    "always_run": AlwaysRunStep,
    "ping": PingStep,
    "cmd": CmdStep,
    "append": AppendStep,
    "breakpoint": BreakpointStep,
    "fake": FakeStep,
}


def concrete_step(v: Any) -> Step:
    if not isinstance(v, dict):
        raise ValueError("step is not a dict")
    if len(v) != 1:
        raise ValueError("step dict must contain a single element")
    step_name, args = v.popitem()
    if step_name not in STEP_TO_CLASS:
        raise ValueError(f"unknown step: {step_name}")
    step_class = STEP_TO_CLASS[step_name]
    if isinstance(args, dict):
        return step_class(**args)
    else:
        first_not_in_step = next(
            (
                k
                for k in step_class.model_fields.keys()
                if k not in Step.model_fields.keys()
            ),
            "__root__",
        )
        return step_class(**{first_not_in_step: args})


AnnotatedStep = Annotated[Step, BeforeValidator(concrete_step)]


class PipelineModel(BaseModel):
    steps: List[AnnotatedStep] = []
    current_step: Optional[Step] = None

    _pipeline_check: PipelineCheck

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @staticmethod
    def run_steps(
        steps: Iterable[Step], pipeline_check: PipelineCheck
    ) -> DataCheckResult:
        for step in steps:
            try:
                result = step.run_step(pipeline_check)
                if not result:
                    PipelineModel.process_always_run(steps, pipeline_check)
                    return DataCheckResult(
                        passed=False,
                        source=f"pipeline {pipeline_check.check_path}",
                    )
            except Exception as e:
                PipelineModel.process_always_run(steps, pipeline_check)
                return pipeline_check.data_check.output.prepare_result(
                    result_type=ResultType.FAILED_WITH_EXCEPTION,
                    source=pipeline_check.check_path,
                    exception=e,
                )

        return DataCheckResult(
            passed=True,
            source=f"pipeline {pipeline_check.check_path}",
        )

    def run(self, pipeline_check: PipelineCheck) -> DataCheckResult:
        self._pipeline_check = pipeline_check
        return PipelineModel.run_steps(self.steps_iterator, pipeline_check)

    @property
    def steps_iterator(self) -> Iterator[Step]:
        for step in self.steps:
            if not step.has_run:
                self.current_step = step
                yield step

    @property
    def next_step(self) -> Optional[Step]:
        for step in self.steps:
            if not step.has_run:
                return step
        else:
            return None

    def run_next_step(self) -> bool:
        next_step = self.next_step
        self.current_step = next_step

        if next_step:
            return next_step.run_step(self._pipeline_check)
        return True

    @staticmethod
    def process_always_run(steps: Iterable[Step], pipeline_check: PipelineCheck):
        for current_step in steps:
            if isinstance(current_step, AlwaysRunStep) and not current_step.has_run:
                with suppress(BaseException):
                    current_step.run(pipeline_check)

    @model_validator(mode="before")
    @classmethod
    def init_pipeline(cls, values):
        if values.get("steps") is None:
            # allow an empty steps entry
            values["steps"] = []
        return values

    def validate_pipeline(self, pipeline_check: PipelineCheck) -> bool:
        for step in self.steps:
            if not step.validate_step(pipeline_check):
                return False
        return True

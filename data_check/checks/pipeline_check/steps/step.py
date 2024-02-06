from __future__ import annotations

from pathlib import Path
from typing import Any, List, Union, cast

from pydantic import BaseModel, ConfigDict, model_validator

from ..pipeline_check import PipelineCheck

StrOrPathList = Union[str, List[Any], Path, List[Path]]


class Step(BaseModel):
    has_run: bool = False

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    def run(self, pipeline_check: PipelineCheck) -> bool:
        return False

    def run_step(self, pipeline_check: PipelineCheck) -> bool:
        self.has_run = True
        result = self.run(pipeline_check)
        return result

    @model_validator(mode="after")
    @classmethod
    def init_step(cls, values):
        return values

    @staticmethod
    def to_path_list(v: Any) -> list[Path]:
        if isinstance(v, str):
            return [Path(v)]
        elif isinstance(v, Path):
            return [v]
        elif isinstance(v, list):
            return [Path(e) for e in v]
        else:
            raise ValueError(f"unknown list format: {v}")

    @staticmethod
    def as_path_list(v: StrOrPathList) -> list[Path]:
        assert isinstance(v, List)
        for e in v:
            assert isinstance(e, Path)
        return cast(List[Path], v)

    @staticmethod
    def as_path(v: Union[str, Path]) -> Path:
        assert isinstance(v, Path)
        return cast(Path, v)

    def validate_step(self, pipeline_check: PipelineCheck) -> bool:
        return True

    @staticmethod
    def validate_path_list_exists(path_list: list[Path], base_path: Path) -> bool:
        for path in path_list:
            return Step.validate_path_exists(path, base_path)
        return True

    @staticmethod
    def validate_path_exists(path: Path, base_path: Path) -> bool:
        try_path = base_path / path
        if not try_path.exists():
            raise FileNotFoundError(f"file not found: {try_path}")
        return True

    @staticmethod
    def base_path(pipeline_check: PipelineCheck) -> Path:
        return pipeline_check.check_path

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Union

from pydantic import BaseModel, Extra, root_validator

if TYPE_CHECKING:
    from data_check import DataCheck

from ..pipeline_check import PipelineCheck

StrOrPathList = Union[str, List[Any], Path, List[Path]]


class Step(BaseModel):
    pipeline_check: PipelineCheck
    base_path: Path = Path()
    has_run: bool = False

    def run(self) -> bool:
        return False

    @staticmethod
    def set_base_path(values: Dict[str, Any]):
        pipeline_check: PipelineCheck = values["pipeline_check"]

        if values["base_path"] == Path():
            values["base_path"] = pipeline_check.check_path

    @root_validator(pre=False)
    def init_step(cls, values):
        cls.set_base_path(values)
        return values

    @property
    def data_check(self) -> DataCheck:
        return self.pipeline_check.data_check

    @staticmethod
    def to_path_list(v) -> List[Path]:
        if isinstance(v, str):
            return [Path(v)]
        elif isinstance(v, Path):
            return [v]
        elif isinstance(v, list):
            return [Path(e) for e in v]
        else:
            raise ValueError(f"unknown list format: {v}")

    class Config:
        arbitrary_types_allowed = True
        extra = Extra.forbid

from pathlib import Path
from typing import List, Union

from pydantic import validator

from .step import Step, StrOrPathList


class FakeStep(Step):
    configs: StrOrPathList
    output: Union[str, Path] = Path()
    force: bool = False

    @validator("output")
    def output_to_path(cls, v) -> Path:
        return Path(v)

    @validator("configs")
    def configs_to_path_list(cls, v) -> List[Path]:
        return Step.to_path_list(v)

    def run(self):
        return self.data_check.fake_data(
            configs=Step.as_path_list(self.configs),
            output=Step.as_path(self.output),
            base_path=self.base_path,
            force=self.force,
        )

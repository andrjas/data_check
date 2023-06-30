from pathlib import Path
from typing import List, Optional, Union

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
            configs=self.configs,
            output=self.output,
            base_path=self.base_path,
            force=self.force,
        )

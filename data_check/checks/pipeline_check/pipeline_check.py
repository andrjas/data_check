from __future__ import annotations

from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union, cast

from data_check.result import ResultType

from ...file_ops import read_yaml
from ...result import DataCheckResult
from ..base_check import BaseCheck

if TYPE_CHECKING:
    from data_check import DataCheck

    from .pipeline_model import PipelineModel


DATA_CHECK_PIPELINE_FILE = "data_check_pipeline.yml"

StrOrPathList = Union[str, List[str], Path, List[Path]]


class PipelineCheck(BaseCheck):
    def __init__(self, data_check: DataCheck, check_path: Path) -> None:
        if check_path.name == DATA_CHECK_PIPELINE_FILE:
            check_path = check_path.parent
        super().__init__(data_check, check_path)
        self._pipeline_config: Optional[Dict] = None

    @staticmethod
    def is_check_path(path: Path) -> bool:
        return (path.is_dir() and (path / DATA_CHECK_PIPELINE_FILE).exists()) or (
            path.name == DATA_CHECK_PIPELINE_FILE and path.exists()
        )

    def _parse_pipeline_file(self, pipeline_file: Path) -> Dict[str, Any]:
        if pipeline_file.exists():
            yaml = read_yaml(
                pipeline_file,
                template_data=dict(
                    self.template_parameters(pipeline_file.parent),
                    **self.data_check.template_data,
                ),
            )
            return yaml or {}  # yaml can return None, so convert it to an empty dict
        return {}

    @property
    def pipeline_config(self):
        if self._pipeline_config is None:
            self._pipeline_config = self._parse_pipeline_file(
                self.check_path / DATA_CHECK_PIPELINE_FILE
            )
        return self._pipeline_config

    @cached_property
    def pipeline_model(self) -> PipelineModel:
        from .pipeline_model import PipelineModel

        return PipelineModel(**self.pipeline_config)

    def validate(self) -> bool:
        # creating the model will validate it
        return self.pipeline_model.validate_pipeline(self)

    def run_test(self) -> DataCheckResult:
        try:
            return self.pipeline_model.run(self)
        except Exception as e:
            return self.data_check.output.prepare_result(
                result_type=ResultType.FAILED_WITH_EXCEPTION,
                source=self.check_path,
                exception=e,
            )

    def template_parameters(self, pipeline_path: Path) -> Dict[str, str]:
        return {
            "CONNECTION": cast(str, self.data_check.config.connection_name),
            "CONNECTION_STRING": cast(str, self.data_check.config.connection),
            "PIPELINE_PATH": str(pipeline_path.absolute()),
            "PIPELINE_NAME": str(pipeline_path.name),
            "PROJECT_PATH": str(self.data_check.config.project_path),
        }

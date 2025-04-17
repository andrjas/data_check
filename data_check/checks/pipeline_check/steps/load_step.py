from pathlib import Path
from typing import Union

from pydantic import model_validator

from data_check.sql import DataCheckSql
from data_check.sql.load_mode import LoadMode
from data_check.utils.deprecation import deprecated_method

from ..pipeline_check import PipelineCheck
from .step import Step, StrOrPathList


class LoadStep(Step):
    files: StrOrPathList = []  # noqa: RUF012
    table: str = ""
    file: Union[str, Path] = ""
    mode: Union[str, LoadMode] = LoadMode.DEFAULT

    load_mode: Union[str, LoadMode, None] = None

    @model_validator(mode="before")
    @classmethod
    def parse_table_or_files_validator(cls, values):
        return LoadStep.parse_table_or_files(cls, values)

    @staticmethod
    def parse_table_or_files(klass, values):
        if "files" in values:
            # file mode, other options are not allowed
            if "file" in values:
                raise ValueError("load: cannot use files and file at the same time")
            if "table" in values:
                raise ValueError("load: cannot use files and table at the same time")
            values["files"] = klass.to_path_list(values["files"])

        if "table" in values:
            # table mode
            if "file" not in values:
                raise ValueError("load: file is missing")
            assert isinstance(values["file"], str)
            values["file"] = Path(values["file"])
        elif "file" in values:
            # use file as an alias for files
            values["files"] = klass.to_path_list(values["file"])
        return values

    def run(self, pipeline_check: PipelineCheck):
        if self.files:
            return self.run_files(
                pipeline_check.data_check.sql, self.base_path(pipeline_check)
            )
        return self.run_table(
            pipeline_check.data_check.sql, self.base_path(pipeline_check)
        )

    def run_files(self, sql: DataCheckSql, base_path: Path):
        return sql.table_loader.load_tables_from_files(
            files=Step.as_path_list(self.files),
            mode=self.mode,
            base_path=base_path,
            load_mode=self.load_mode,
        )

    def run_table(self, sql: DataCheckSql, base_path: Path):
        return sql.table_loader.load_table_from_file(
            table=self.table,
            file=Step.as_path(self.file),
            mode=self.mode,
            base_path=base_path,
            load_mode=self.load_mode,
        )

    def validate_step(self, pipeline_check: PipelineCheck) -> bool:
        if self.files:
            return Step.validate_path_list_exists(
                self.as_path_list(self.files), self.base_path(pipeline_check)
            )
        elif self.file:
            return Step.validate_path_exists(
                self.as_path(self.file), self.base_path(pipeline_check)
            )
        return True


class DeprecatedLoadTableStep(LoadStep):
    @model_validator(mode="after")
    @classmethod
    def parse_table_or_files_validator(cls, values):
        deprecated_method(
            "load_table",
            """
load:
    table: ...
    file: ...
""",
        )
        return LoadStep.parse_table_or_files(cls, values)

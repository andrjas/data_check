from pathlib import Path
from typing import Union

from pydantic import model_validator

from data_check import DataCheck
from data_check.utils.deprecation import deprecated_method

from ..pipeline_check import PipelineCheck
from .step import Step, StrOrPathList


class SqlStep(Step):
    query_or_files: StrOrPathList = ""
    query: str = ""
    output: Union[str, Path] = ""
    print_query: bool = True
    files: StrOrPathList = ""
    file: StrOrPathList = ""
    write_check: Union[str, Path] = ""

    @model_validator(mode="before")
    @classmethod
    def parse_query_or_files_validator(cls, values):
        return SqlStep.parse_query_or_files(cls, values)

    @staticmethod
    def parse_query_or_files(klass, values):
        if "file" in values:
            if "files" in values:
                raise ValueError("cannot use files and file at the same time")
            values["files"] = klass.to_path_list(values["file"])
        elif "files" in values:
            values["files"] = klass.to_path_list(values["files"])

        if "query" in values and "files" in values:
            raise ValueError("cannot use query and files at the same time")
        return values

    def prepare_query_or_files(self, base_path: Path):
        if self.query_or_files:
            if isinstance(self.query_or_files, str):
                try_file = base_path / self.query_or_files
                if try_file.exists():
                    self.files = self.to_path_list(self.query_or_files)
                else:
                    self.query = self.query_or_files
                self.query_or_files = ""
            elif isinstance(self.query_or_files, list):
                self.files = self.to_path_list(self.query_or_files)

    def run(self, pipeline_check: PipelineCheck):
        self.prepare_query_or_files(self.base_path(pipeline_check))
        if self.query:
            return self.run_query(
                pipeline_check.data_check, self.base_path(pipeline_check)
            )
        return self.run_files(pipeline_check.data_check, self.base_path(pipeline_check))

    def run_query(self, data_check: DataCheck, base_path: Path):
        if self.write_check:
            return data_check.write_check(
                query=self.query,
                check_path=Path(self.write_check),
                base_path=base_path,
            )
        return data_check.run_sql_query(
            query=self.query,
            output=self.output,
            base_path=base_path,
            print_query=self.print_query,
        )

    def run_files(self, data_check: DataCheck, base_path: Path):
        return data_check.run_sql_files(
            files=Step.as_path_list(self.files), base_path=base_path
        )

    def validate_step(self, pipeline_check: PipelineCheck) -> bool:
        self.prepare_query_or_files(self.base_path(pipeline_check))
        if self.files:
            return Step.validate_path_list_exists(
                self.as_path_list(self.files), self.base_path(pipeline_check)
            )
        return True


class DeprecatedSqlFilesStep(SqlStep):
    @model_validator(mode="after")
    @classmethod
    def parse_query_or_files_validator(cls, values):
        deprecated_method(
            "sql_files",
            """
sql:
    files:
""",
        )
        return SqlStep.parse_query_or_files(cls, values)

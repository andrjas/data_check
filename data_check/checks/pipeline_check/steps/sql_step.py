from pathlib import Path
from typing import Union

from pydantic import root_validator

from data_check.utils.deprecation import deprecated_method

from .step import Step, StrOrPathList


class SqlStep(Step):
    query_or_files: StrOrPathList = ""
    query: str = ""
    output: Union[str, Path] = ""
    print_query: bool = True
    files: StrOrPathList = ""
    file: StrOrPathList = ""
    write_check: Union[str, Path] = ""

    @root_validator(pre=False)
    def parse_query_or_files(cls, values):
        # if values['base_path'] == Path():

        if values["query_or_files"] and not values["query"]:
            if isinstance(values["query_or_files"], str):
                try_file = values["base_path"] / values["query_or_files"]
                if try_file.exists():
                    values["files"] = values["query_or_files"]
                else:
                    values["query"] = values["query_or_files"]

        if values["file"]:
            if values["files"]:
                raise ValueError(f"cannot use files and file at the same time")
            values["files"] = cls.to_path_list(values["file"])
        elif values["files"]:
            values["files"] = cls.to_path_list(values["files"])

        if values["query"] and values["files"]:
            raise ValueError(f"cannot use query and files at the same time")

        return values

    def run(self):
        if self.query:
            return self.run_query()
        return self.run_files()

    def run_query(self):
        if self.write_check:
            return self.data_check.write_check(
                query=self.query,
                check_path=Path(self.write_check),
                base_path=self.base_path,
            )
        return self.data_check.run_sql_query(
            query=self.query,
            output=self.output,
            base_path=self.base_path,
            print_query=self.print_query,
        )

    def run_files(self):
        return self.data_check.run_sql_files(files=self.files, base_path=self.base_path)


class DeprecatedSqlFilesStep(SqlStep):
    @root_validator(pre=False)
    def parse_query_or_files(cls, values):
        deprecated_method(
            "sql_files",
            """
sql:
    files:
""",
        )
        return super().parse_query_or_files(cls, values)

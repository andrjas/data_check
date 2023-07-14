from pathlib import Path
from typing import Union

from pydantic import root_validator

from data_check.sql.load_mode import LoadMode
from data_check.utils.deprecation import deprecated_method

from .step import Step, StrOrPathList


class LoadStep(Step):
    files: StrOrPathList = []
    table: str = ""
    file: Union[str, Path] = ""
    mode: Union[str, LoadMode] = LoadMode.DEFAULT

    load_mode: Union[str, LoadMode, None] = None

    @root_validator(pre=False)
    def parse_table_or_files(cls, values):
        # if values['base_path'] == Path():

        if values["files"]:
            if values["file"]:
                raise ValueError("load: cannot use files and file at the same time")
            if values["table"]:
                raise ValueError("load: cannot use files and table at the same time")
            values["files"] = cls.to_path_list(values["files"])

        if values["table"]:
            if not values["file"]:
                raise ValueError("load: file is missing")
            assert isinstance(values["file"], str)
            values["file"] = Path(values["file"])
        elif values["file"]:
            values["files"] = cls.to_path_list(values["file"])

        return values

    def run(self):
        if self.files:
            return self.run_files()
        return self.run_table()

    def run_files(self):
        return self.data_check.sql.table_loader.load_tables_from_files(
            files=Step.as_path_list(self.files),
            mode=self.mode,
            base_path=self.base_path,
            load_mode=self.load_mode,
        )

    def run_table(self):
        return self.data_check.sql.table_loader.load_table_from_file(
            table=self.table,
            file=Step.as_path(self.file),
            mode=self.mode,
            base_path=self.base_path,
            load_mode=self.load_mode,
        )


class DeprecatedLoadTableStep(LoadStep):
    @root_validator(pre=False)
    def parse_table_or_files(cls, values):
        deprecated_method(
            "load_table",
            """
load:
    table: ...
    file: ...
""",
        )
        return super().parse_table_or_files(cls, values)

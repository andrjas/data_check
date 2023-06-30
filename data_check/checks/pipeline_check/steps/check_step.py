from pydantic import validator

from .step import Step, StrOrPathList


class CheckStep(Step):
    files: StrOrPathList

    def run(self):
        return self.data_check.run(files=self.files, base_path=self.base_path)

    @validator("files")
    def files_to_list(cls, v):
        return cls.to_path_list(v)

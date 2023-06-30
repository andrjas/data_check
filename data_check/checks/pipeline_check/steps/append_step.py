from typing import Union

from data_check.sql.load_mode import LoadMode

from .load_step import LoadStep


class AppendStep(LoadStep):
    mode: Union[str, LoadMode] = LoadMode.APPEND

from pathlib import Path
import pandas as pd
from typing import Union, List, cast

from .csv_check import CSVCheck
from ..result import DataCheckResult, ResultType


class ExcelCheck(CSVCheck):
    @staticmethod
    def is_check_path(path: Path):
        return path.suffix.lower() == ".sql" and path.with_suffix(".xlsx").exists()

    def get_expect_file(self, sql_file: Path) -> Path:
        return sql_file.with_suffix(".xlsx")

    def clean_string_column(self, col: str):
        return col.replace(u"\u00A0", " ")

    def clean_excel_df(self, df: pd.DataFrame) -> pd.DataFrame:
        for column_name, column in df.items():
            try:
                if column.dtype == "object":
                    _col = column.apply(self.clean_string_column)
                    df[column_name] = _col
            except Exception:
                pass
        return df

    def read_expect_file(
        self, expect_file: Path, string_columns: List[str]
    ) -> Union[DataCheckResult, pd.DataFrame]:
        try:
            expect_result: pd.DataFrame = pd.read_excel(
                cast(str, expect_file),
                sheet_name=0,
                header=0,
                engine="openpyxl",
                dtype="object",
            )
            return self.clean_excel_df(expect_result)
        except Exception as exc_csv:
            return self.data_check.output.prepare_result(
                ResultType.FAILED_WITH_EXCEPTION, source=expect_file, exception=exc_csv
            )

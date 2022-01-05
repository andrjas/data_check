from __future__ import annotations
from pathlib import Path
from typing import Union, List, cast, TYPE_CHECKING

from .sql_base_check import SQLBaseCheck
from ..result import DataCheckResult, ResultType
from data_check.sql.query_result import QueryResult
from .csv_check import CSVCheck
from .excel_check import ExcelCheck

if TYPE_CHECKING:
    from data_check import DataCheck


class TableCheck(SQLBaseCheck):
    def __init__(self, data_check: DataCheck, check_path: Path) -> None:
        super().__init__(data_check, check_path)
        self.check_path_sql = self.check_path.with_suffix(".sql")
        self.check_instance = self.get_check_instance()
        self.check_instance.get_sql_result = self.get_sql_result

    @staticmethod
    def is_check_path(path: Path):
        return (
            path.suffix.lower() in (".csv", ".xlsx")
            and not path.with_suffix(".sql").exists()
        )

    def get_check_instance(self) -> Union[CSVCheck, ExcelCheck]:
        if self.check_path.suffix.lower() == ".csv":
            return CSVCheck(self.data_check, self.check_path_sql)
        elif self.check_path.suffix.lower() == ".xlsx":
            return ExcelCheck(self.data_check, self.check_path_sql)
        else:
            raise Exception(f"unsupported table check file: {self.check_path}")

    def get_sql_result(self) -> Union[DataCheckResult, QueryResult]:
        expect_result = self.check_instance.read_expect_file(
            self.check_instance.get_expect_file(self.check_path_sql), string_columns=[]
        )
        column_list: List[str] = cast(List[str], expect_result.columns.tolist())
        table_name = self.check_path.stem
        query = f"select {','.join(column_list)} from {table_name}"
        try:
            return self.data_check.sql.run_query_with_result(
                query, params=self.data_check.sql_params
            )
        except Exception as exc:
            return self.data_check.output.prepare_result(
                ResultType.FAILED_WITH_EXCEPTION, source=self.check_path, exception=exc
            )

    def run_test(self) -> DataCheckResult:
        return self.check_instance.run_test()

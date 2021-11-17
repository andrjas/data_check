from pathlib import Path

from ..result import DataCheckResult, ResultType
from .base_check import BaseCheck
from ..io import read_sql_file


class EmptySetCheck(BaseCheck):
    @staticmethod
    def is_check_path(path: Path):
        return path.suffix.lower() == ".sql" and path.with_suffix(".empty").exists()

    def run_test(self) -> DataCheckResult:
        try:
            query = read_sql_file(
                sql_file=self.check_path, template_data=self.data_check.template_data
            )
            sql_result = self.data_check.sql.run_query_with_result(
                query, params=self.data_check.sql_params
            )
            if len(sql_result) == 0:
                return self.data_check.output.prepare_result(
                    ResultType.PASSED, source=self.check_path, result=sql_result.df
                )
            else:
                return self.data_check.output.prepare_result(
                    ResultType.FAILED_DIFFERENT_LENGTH, source=self.check_path
                )
        except Exception as exc:
            return self.data_check.output.prepare_result(
                ResultType.FAILED_WITH_EXCEPTION, source=self.check_path, exception=exc
            )

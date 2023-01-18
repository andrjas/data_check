from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict

from ..io import get_expect_file, read_sql_file, rel_path, write_csv
from ..result import DataCheckResult
from .base_check import BaseCheck

if TYPE_CHECKING:
    from data_check import DataCheck


class DataCheckGenerator(BaseCheck):
    """Special type of a "check" to generate the expectation files."""

    def __init__(self, data_check: DataCheck, check_path: Path) -> None:
        if check_path.suffix.lower() in (".csv", ".xlsx"):
            check_path = check_path.with_suffix(".sql")
        super().__init__(data_check, check_path)

    def gen_expectation(
        self, sql_file: Path, force: bool = False, template_data: Dict[str, Any] = {}
    ) -> DataCheckResult:
        """
        Executes the query for a data_check test
        and stores the result in the csv file.
        """
        expect_result = get_expect_file(sql_file)
        _rel_path = rel_path(expect_result)
        if not expect_result.exists() or force:
            result = self.data_check.sql.run_query(
                read_sql_file(sql_file=sql_file, template_data=template_data),
                params=self.data_check.sql_params,
            )
            write_csv(result, expect_result)
            output = f"expectation written to {_rel_path}"
        else:
            output = f"expectation skipped for {_rel_path}"
        return DataCheckResult(passed=True, source=sql_file, result=output)

    def run_test(self) -> DataCheckResult:
        return self.gen_expectation(
            sql_file=self.check_path,
            force=self.data_check.config.force,
            template_data=self.data_check.template_data,
        )

from pathlib import Path
from typing import Dict, Any

from .base_check import BaseCheck

from ..io import get_expect_file, read_sql_file, rel_path
from ..result import DataCheckResult


class DataCheckGenerator(BaseCheck):
    """Special type of a "check" to generate the expectation files."""

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
                read_sql_file(sql_file=sql_file, template_data=template_data)
            )
            result.to_csv(expect_result, index=False)
            output = f"expectation written to {_rel_path}"
        else:
            output = f"expectation skipped for {_rel_path}"
        return DataCheckResult(True, output, output)

    def run_test(self) -> DataCheckResult:
        return self.gen_expectation(
            sql_file=self.check_path,
            force=self.data_check.config.force,
            template_data=self.data_check.template_data,
        )

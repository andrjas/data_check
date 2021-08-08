from pathlib import Path
from typing import List, Dict, Any

from .sql import DataCheckSql
from .io import get_expect_file, read_sql_file, rel_path


class DataCheckGenerator:
    def __init__(self, sql: DataCheckSql) -> None:
        self.sql = sql

    def gen_expectation(
        self, sql_file: Path, force: bool = False, template_data: Dict[str, Any] = {}
    ):
        """
        Executes the query for a data_check test
        and stores the result in the csv file.
        """
        expect_result = get_expect_file(sql_file)
        _rel_path = rel_path(expect_result)
        if not expect_result.exists() or force:
            result = self.sql.run_query(
                read_sql_file(sql_file=sql_file, template_data=template_data)
            )
            result.to_csv(expect_result, index=False)
            print(f"expectation written to {_rel_path}")
        else:
            print(f"expectation skipped for {_rel_path}")

    def generate_expectations(
        self, files: List[Path], force: bool = False, template_data: Dict[str, Any] = {}
    ):
        """
        Generated a expected results file for each file if it doesn't exists yet.
        """
        for file in files:
            if file.is_file() and file.suffix.lower() == ".sql":
                self.gen_expectation(file, force, template_data)

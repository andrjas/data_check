from pathlib import Path
from .io import get_expect_file
from typing import List

from .sql import DataCheckSql


class DataCheckGenerator:
    def __init__(self, sql: DataCheckSql) -> None:
        self.sql = sql

    def gen_expectation(self, sql_file: Path, force: bool = False):
        """
        Executes the query for a data_check test
        and stores the result in the csv file.
        """
        expect_result = get_expect_file(sql_file)
        if not expect_result.exists() or force:
            result = self.sql.run_query(sql_file.read_text(encoding="UTF-8"))
            result.to_csv(expect_result, index=False)
            print(f"expectation written to {expect_result}")
        else:
            print(f"expectation skipped for {expect_result}")

    def generate_expectations(self, files: List[Path], force: bool = False):
        """
        Generated a expected results file for each file if it doesn't exists yet.
        """
        for file in files:
            if file.is_file() and file.suffix.lower() == ".sql":
                self.gen_expectation(file, force)

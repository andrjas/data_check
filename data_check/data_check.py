from pathlib import Path
import yaml
import pandas as pd
from typing import Union


class DataCheckException(Exception):
    """
    Generic class for various exceptions that might occur in data_check.
    """

    pass


class DataCheckResult:
    """
    Wrapper class holding the result of a test run.
    """

    def __init__(self, passed: bool, result: Union[pd.DataFrame, str]):
        self.passed = passed
        self.result = result

    def __bool__(self):
        return self.passed


class DataCheck:
    """
    Main class for data_check.
    """

    @staticmethod
    def read_config(config_path: Path = Path("data_check.yml")):
        config = yaml.safe_load(config_path.open())
        return config

    def __init__(self, connection: str):
        self.connection = connection

    def run_query(self, query: str) -> pd.DataFrame:
        """
        Run a query on the database and return a Pandas DataFrame with the result.
        """
        if not self.connection:
            raise DataCheckException(f"undefined connection: {self.connection}")
        return pd.read_sql(query, self.connection)

    def get_expect_file(self, sql_file: Path) -> Path:
        """
        Returns the csv file with the expected results for a sql file.
        """
        return sql_file.parent / (sql_file.stem + ".csv")

    def run_test(self, sql_file: Path, return_all=False) -> DataCheckResult:
        """
        Run a data_check test on a single input file.
        Returns a DataCheckResult with the result.

        If return_all is set, the DataCheckResult will contail all results,
        not only the failed ones.
        """
        expect_file = self.get_expect_file(sql_file)
        sql_result = self.run_query(sql_file.read_text())
        expect_result = pd.read_csv(expect_file)

        # replace missing values and None with pd.NA
        sql_result.fillna(value=pd.NA, inplace=True)
        expect_result.fillna(value=pd.NA, inplace=True)

        df_merged = sql_result.merge(expect_result, indicator=True, how="outer")
        df_diff = df_merged[df_merged._merge != "both"]

        if len(df_diff) != 0:
            print(f"{sql_file}: FAILED")
            passed = False
        else:
            print(f"{sql_file}: PASSED")
            passed = True

        if return_all:
            return DataCheckResult(passed=passed, result=df_merged)
        else:
            return DataCheckResult(passed=passed, result=df_diff)

    def run(self, any_path: Path) -> bool:
        """
        Run a data_check test on a file or folder.
        In case of a file, it will just call run_test.
        For a folder, it will scan the folder recursively and run a test for each input file.

        Returns True if all tests passed, otherwise False.
        """
        if any_path.is_file():
            return self.run_test(any_path)
        elif any_path.is_dir():
            all_files = any_path.glob("**/*.sql")
            return all([self.run_test(sql_file) for sql_file in all_files])
        else:
            raise Exception(f"unexpected path: {any_path}")

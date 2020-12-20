from pathlib import Path
import yaml
import pandas as pd
from typing import Union, List
import concurrent.futures


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

    def __init__(self, connection: str, workers=4):
        self.connection = connection
        self.workers = workers

    @property
    def executor(self):
        return concurrent.futures.ThreadPoolExecutor(max_workers=self.workers)

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

        try:
            sql_result = self.run_query(sql_file.read_text())
        except Exception as exc:
            print(f"{sql_file}: FAILED (with exception in {sql_file})")
            return DataCheckResult(
                passed=False, result=f"{sql_file} generated an exception: {exc}"
            )

        try:
            expect_result = pd.read_csv(expect_file)
        except Exception as exc_csv:
            print(f"{sql_file}: FAILED (with exception in {expect_file})")
            return DataCheckResult(
                passed=False, result=f"{expect_file} generated an exception: {exc_csv}"
            )

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

    def expand_files(self, files: List[Path]) -> List[Path]:
        """
        Expands the list of files or folders,
        with all SQL files in a folder as seperate files.
        """
        result = []
        for f in files:
            if f.is_file():
                result.append(f)
            elif f.is_dir():
                result.extend(f.glob("**/*.sql"))
            else:
                raise Exception(f"unexpected path: {f}")
        return result

    def run(self, files: List[Path]) -> bool:
        """
        Runs a data_check test for all element in the list in parallel.
        Returns True, if all calls returned True, otherweise False.
        """
        all_files = self.expand_files(files)
        result_futures = [self.executor.submit(self.run_test, f) for f in all_files]
        results = []
        for future in concurrent.futures.as_completed(result_futures):
            results.append(future.result())
        return all(results)

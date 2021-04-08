from pathlib import Path
from pandas.core.frame import DataFrame
import yaml
import pandas as pd
from typing import Union, List, Dict
import concurrent.futures
from sqlalchemy import create_engine
import traceback
from colorama import Fore, Style
from os import linesep, path
from jinja2 import Template


class DataCheckException(Exception):
    """
    Generic class for various exceptions that might occur in data_check.
    """

    pass


class DataCheckResult:
    """
    Wrapper class holding the result of a test run.
    """

    def __init__(
        self, passed: bool, result: Union[pd.DataFrame, str], message: str = ""
    ):
        self.passed = passed
        self.result = result
        self.message = message

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

    def __init__(self, connection: str, workers=4, verbose=False, traceback=False):
        self.connection = connection
        self.workers = workers
        self.verbose = verbose
        self.traceback = traceback
        self.template_data = {}

    def load_template(self):
        template_yaml = Path("checks") / "template.yml"
        if template_yaml.exists():
            self.template_data = yaml.safe_load(template_yaml.open())

    @property
    def executor(self):
        return concurrent.futures.ProcessPoolExecutor(max_workers=self.workers)

    def get_db_params(self) -> Dict:
        """
        Return parameter specific to a database.
        """
        return {}  # no special parameters needed for now

    def get_engine(self, extra_params={}):
        """
        Return the database engine for the connection.
        """
        return create_engine(
            path.expandvars(self.connection), **{**self.get_db_params(), **extra_params}
        )

    def run_query(self, query: str) -> pd.DataFrame:
        """
        Run a query on the database and return a Pandas DataFrame with the result.
        """
        if not self.connection:
            raise DataCheckException(f"undefined connection: {self.connection}")
        with self.get_engine().connect() as connection:
            return pd.read_sql_query(query, connection)

    def get_expect_file(self, sql_file: Path) -> Path:
        """
        Returns the csv file with the expected results for a sql file.
        """
        return sql_file.parent / (sql_file.stem + ".csv")

    def print_failed(self, df: pd.DataFrame, print_format="pandas") -> str:
        """
        Prints a DataFrame with diff information and returns it as a string.
        """
        with pd.option_context("display.max_rows", None, "display.max_columns", None):
            df["_diff"] = ""
            df.loc[df._merge == "left_only", ["_diff"]] = "db"
            df.loc[df._merge == "right_only", ["_diff"]] = "expected"
            if print_format == "pandas":
                return str(df.drop(["_merge"], axis=1))
            elif print_format.lower() == "csv":
                return df.drop(["_merge"], axis=1).to_csv(index=False)
            else:
                raise DataCheckException(f"unknown print format: {print_format}")

    def merge_results(
        self, sql_result: DataFrame, expect_result: DataFrame
    ) -> DataFrame:
        """
        Merges the results of a SQL query and the expected results.
        Returns the merged DataFrame.
        """
        try:
            df_merged = sql_result.merge(expect_result, indicator=True, how="outer")
        except ValueError:
            # treat both columns as str if their data types differ
            for sc in sql_result.columns:
                if sc in expect_result.columns:
                    if sql_result[sc].dtype != expect_result[sc].dtype:
                        expect_result[sc] = expect_result[sc].astype("str")
                        sql_result[sc] = sql_result[sc].astype("str")
            df_merged = sql_result.merge(expect_result, indicator=True, how="outer")
        return df_merged

    @staticmethod
    def str_pass(string):
        return Fore.GREEN + string + Style.RESET_ALL

    @staticmethod
    def str_warn(string):
        return Fore.YELLOW + string + Style.RESET_ALL

    @staticmethod
    def str_fail(string):
        return Fore.RED + string + Style.RESET_ALL

    def read_sql_file(self, sql_file) -> str:
        """
        Reads the SQL file and returns it as a string.
        Evaluates the templates when needed.
        """
        return Template(sql_file.read_text(encoding="UTF-8")).render(
            **self.template_data
        )

    def run_test(
        self,
        sql_file: Path,
        return_all=False,
        print_failed=False,
        print_format="pandas",
    ) -> DataCheckResult:
        """
        Run a data_check test on a single input file.
        Returns a DataCheckResult with the result.

        If return_all is set, the DataCheckResult will contail all results,
        not only the failed ones.
        """
        expect_file = self.get_expect_file(sql_file)
        if not expect_file.exists():
            warn = self.str_warn("NO EXPECTED RESULTS FILE")
            message = f"{sql_file}: {warn}"
            return DataCheckResult(
                passed=False,
                result=f"{sql_file}: NO EXPECTED RESULTS FILE",
                message=message,
            )  # no need to run queries, if no expected results found

        try:
            sql_result = self.run_query(self.read_sql_file(sql_file))
        except Exception as exc:
            fail = self.str_fail(f"FAILED (with exception in {sql_file})")
            message = f"{sql_file}: {fail}"
            if self.verbose:
                message += linesep + str(exc)
            if self.traceback:
                message += linesep + traceback.format_exc()
            return DataCheckResult(
                passed=False,
                result=f"{sql_file} generated an exception: {exc}",
                message=message,
            )

        try:
            expect_result = pd.read_csv(
                expect_file,
                na_values=[""],  # use empty string as nan
                keep_default_na=False,
                comment="#",
                quotechar='"',
                quoting=0,
                engine="c",
            )
        except Exception as exc_csv:
            fail = self.str_fail(f"FAILED (with exception in {expect_file})")
            message = f"{sql_file}: {fail}"
            if self.verbose:
                message += linesep + str(exc_csv)
            if self.traceback:
                message += linesep + traceback.format_exc()
            return DataCheckResult(
                passed=False,
                result=f"{expect_file} generated an exception: {exc_csv}",
                message=message,
            )

        # replace missing values and None with pd.NA
        sql_result.fillna(value=pd.NA, inplace=True)
        expect_result.fillna(value=pd.NA, inplace=True)

        df_merged = self.merge_results(sql_result, expect_result)
        df_diff = df_merged[df_merged._merge != "both"]

        if len(df_diff) != 0:
            failed = self.str_fail("FAILED")
            message = f"{sql_file}: {failed}"
            if print_failed:
                message += linesep + self.print_failed(df_diff.copy(), print_format)
            passed = False
        else:
            passed_msg = self.str_pass("PASSED")
            message = f"{sql_file}: {passed_msg}"
            passed = True

        if return_all:
            return DataCheckResult(passed=passed, result=df_merged, message=message)
        else:
            return DataCheckResult(passed=passed, result=df_diff, message=message)

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

    def run(self, files: List[Path], print_failed=False, print_format="pandas") -> bool:
        """
        Runs a data_check test for all element in the list.
        Returns True, if all calls returned True, otherweise False.
        """
        all_files = self.expand_files(files)
        if self.workers == 1 or len(all_files) == 1:
            results = self.run_serial(all_files, print_failed, print_format)
        else:
            results = self.run_parallel(all_files, print_failed, print_format)

        overall_result = all(results)
        overall_result_msg = (
            self.str_pass("PASSED") if overall_result else self.str_fail("FAILED")
        )
        print("")
        print(f"overall result: {overall_result_msg}")
        return overall_result

    def run_parallel(self, all_files, print_failed, print_format):
        """
        Runs all tests in parallel.
        Returns a list of the results
        """
        result_futures = [
            self.executor.submit(
                self.run_test, f, print_failed=print_failed, print_format=print_format
            )
            for f in all_files
        ]
        results = []
        for future in concurrent.futures.as_completed(result_futures):
            dc_result = future.result()
            results.append(dc_result)
            print(dc_result.message)
        return results

    def run_serial(self, all_files, print_failed, print_format):
        """
        Runs all tests in serial.
        Returns a list of the results
        """
        results = []
        for f in all_files:
            dc_result = self.run_test(
                f, print_failed=print_failed, print_format=print_format
            )
            results.append(dc_result)
            print(dc_result.message)
        return results

    def gen_expectation(self, sql_file: Path, force=False):
        """
        Executes the query for a data_check test
        and stores the result in the csv file.
        """
        expect_result = self.get_expect_file(sql_file)
        if not expect_result.exists() or force:
            result = self.run_query(sql_file.read_text(encoding="UTF-8"))
            result.to_csv(expect_result, index=False)
            print(f"expectation written to {expect_result}")
        else:
            print(f"expectation skipped for {expect_result}")

    def generate_expectations(self, files: List[Path], force=False):
        """
        Generated a expected results file for each file if it doesn't exists yet.
        """
        for sql_file in self.expand_files(files):
            self.gen_expectation(sql_file, force)

    def test_connection(self) -> bool:
        """
        Returns True if we can connect to the database.
        Mainly for integration tests.
        """
        engine = self.get_engine(extra_params={"pool_pre_ping": True})
        try:
            engine.connect()
            return True
        except:
            return False

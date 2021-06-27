from pathlib import Path
import pandas as pd
from typing import List, Tuple

from .config import DataCheckConfig
from .result import DataCheckResult, ResultType
from .output import DataCheckOutput
from .io import expand_files, read_sql_file, get_expect_file, read_csv, read_yaml
from .sql import DataCheckSql
from .generator import DataCheckGenerator
from .runner import DataCheckRunner


class DataCheck:
    """
    Main class for data_check.
    """

    def __init__(self, config: DataCheckConfig = DataCheckConfig()):
        self.config = config
        self.sql = DataCheckSql(config.connection)
        self.generator = DataCheckGenerator(self.sql)
        self.runner = DataCheckRunner(config.parallel_workers)
        self.output = DataCheckOutput()
        self.template_data = {}

    def load_template(self):
        template_yaml = self.config.checks_path / self.config.tempate_path
        if template_yaml.exists():
            self.template_data = read_yaml(template_yaml)

    @staticmethod
    def merge_results(
        sql_result: pd.DataFrame, expect_result: pd.DataFrame
    ) -> pd.DataFrame:
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
    def get_result(
        sql_result, expect_result, return_all
    ) -> Tuple[ResultType, pd.DataFrame]:
        # replace missing values and None with pd.NA
        sql_result.fillna(value=pd.NA, inplace=True)
        expect_result.fillna(value=pd.NA, inplace=True)

        df_merged = DataCheck.merge_results(sql_result, expect_result)
        df_diff = df_merged[df_merged._merge != "both"]
        assert isinstance(df_diff, pd.DataFrame)  # assert a DataFrame, not a Series
        df_result = df_merged if return_all else df_diff

        # empty diff means there are no differences and the test has passed
        passed = len(df_diff) == 0
        return (DataCheckResult.passed_to_result_type(passed), df_result)

    def run_test(
        self,
        sql_file: Path,
        return_all=False,
    ) -> DataCheckResult:
        """
        Run a data_check test on a single input file.
        Returns a DataCheckResult with the result.

        If return_all is set, the DataCheckResult will contail all results,
        not only the failed ones.
        """
        expect_file = get_expect_file(sql_file)
        if not expect_file.exists():
            # no need to run queries, if no expected results found
            return self.output.prepare_result(
                ResultType.NO_EXPECTED_RESULTS_FILE, source=sql_file
            )

        try:
            sql_result = self.sql.run_query(
                read_sql_file(sql_file=sql_file, template_data=self.template_data)
            )
        except Exception as exc:
            return self.output.prepare_result(
                ResultType.FAILED_WITH_EXCEPTION, source=sql_file, exception=exc
            )

        try:
            expect_result = read_csv(expect_file)
        except Exception as exc_csv:
            return self.output.prepare_result(
                ResultType.FAILED_WITH_EXCEPTION, source=expect_file, exception=exc_csv
            )

        result_type, df_result = self.get_result(sql_result, expect_result, return_all)
        return self.output.prepare_result(
            result_type, source=sql_file, result=df_result
        )

    def run(self, files: List[Path]) -> bool:
        """
        Runs a data_check test for all element in the list.
        Returns True, if all calls returned True, otherweise False.
        """
        all_files = expand_files(files)
        results = self.runner.run(self.run_test, all_files)

        overall_result = all(results)
        self.output.pprint_overall_result(overall_result)
        return overall_result

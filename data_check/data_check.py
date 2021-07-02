from pathlib import Path
import pandas as pd
from typing import List, Tuple, Dict, Any

from .config import DataCheckConfig
from .result import DataCheckResult, ResultType
from .output import DataCheckOutput
from .io import expand_files, read_sql_file, get_expect_file, read_csv, read_yaml
from .sql import DataCheckSql
from .generator import DataCheckGenerator
from .runner import DataCheckRunner
from .test_type.simple_check import SimpleCheck
from .test_type.pipeline_check import PipelineCheck


class DataCheck(SimpleCheck, PipelineCheck):
    """
    Main class for data_check.
    """

    def __init__(self, config: DataCheckConfig = DataCheckConfig()):
        PipelineCheck.__init__(self)
        self.config = config
        self.runner = DataCheckRunner(config.parallel_workers)
        self.sql = DataCheckSql(connection=config.connection, runner=self.runner)
        self.generator = DataCheckGenerator(self.sql)
        self.output = DataCheckOutput()
        self.template_data: Dict[str, Any] = {}

        self.register_pipelines()

    def load_template(self):
        template_yaml = self.config.checks_path / self.config.tempate_path
        if template_yaml.exists():
            self.template_data = read_yaml(template_yaml)

    def delegate_test(self, path: Path) -> DataCheckResult:
        if self.is_pipeline_check(path):
            return self.run_pipeline(path)
        else:
            return self.run_test(path)

    def run(self, files: List[Path]) -> bool:
        """
        Runs a data_check test for all element in the list.
        Returns True, if all calls returned True, otherweise False.
        """
        all_files = expand_files(files)
        results = self.runner.run(self.delegate_test, all_files)

        overall_result = all(results)
        self.output.pprint_overall_result(overall_result)
        return overall_result

    def run_sql_file(self, sql_file: Path):
        sql_text = read_sql_file(sql_file=sql_file, template_data=self.template_data)
        print("executing:")
        print(sql_text)
        return self.sql.run_sql(sql_text=sql_text)

    def run_sql_files(self, sql_files: List[Path]):
        parameters = [{"sql_file": f} for f in expand_files(sql_files)]
        return self.runner.run_any(run_method=self.run_sql_file, parameters=parameters)

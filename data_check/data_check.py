from pathlib import Path
from typing import List, Dict, Any, Union

from .config import DataCheckConfig
from .result import DataCheckResult
from .output import DataCheckOutput
from .io import expand_files, read_sql_file, read_yaml, parse_template
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
        if self.config.template_path.exists():
            self.template_data = read_yaml(self.config.template_path)

    def delegate_test(self, path: Path) -> DataCheckResult:
        if self.is_pipeline_check(path):
            return self.run_pipeline(path)
        elif self.config.generate_mode:
            return self.generator.gen_expectation(
                sql_file=path, force=self.config.force, template_data=self.template_data
            )
        else:
            return self.run_test(path)

    def collect_checks(
        self, files: List[Path], base_path: Path = Path(".")
    ) -> List[Path]:
        base_path = base_path.absolute()
        checks = []
        for f in sorted(files):
            abs_file = f if f.is_absolute() else base_path / f
            if self.is_pipeline_check(abs_file):
                checks.append(abs_file)
            elif self.is_simple_check(abs_file):
                checks.append(abs_file)
            elif abs_file.is_dir():
                dir_files = [d for d in abs_file.iterdir()]
                checks.extend(self.collect_checks(dir_files, base_path=base_path))
        return checks

    def run(self, files: List[Path], base_path: Path = Path(".")) -> bool:
        """
        Runs a data_check test for all element in the list.
        Returns True, if all calls returned True, otherweise False.
        """
        # all_files = expand_files(files, base_path=base_path)
        all_checks = self.collect_checks(files, base_path=base_path)
        results = self.runner.run(self.delegate_test, all_checks)

        overall_result = all(results)
        if self.config.print_overall_result:
            self.output.pprint_overall_result(overall_result)
        return overall_result

    def run_sql_file(self, file: Path):
        sql_text = read_sql_file(sql_file=file, template_data=self.template_data)
        print("executing:")
        print(sql_text)
        return self.sql.run_sql(query=sql_text)

    def run_sql_files(self, files: List[Path], base_path: Path = Path(".")):
        parameters = [{"file": f} for f in expand_files(files, base_path=base_path)]
        return all(
            self.runner.run_any(run_method=self.run_sql_file, parameters=parameters)
        )

    def run_sql_query(
        self,
        query: str,
        output: Union[str, Path] = "",
        base_path: Path = Path("."),
        print_query=False,
    ):
        sql_query = parse_template(query, template_data=self.template_data)
        if print_query:
            print(f"-- {sql_query}")
        return self.sql.run_sql(sql_query, output, base_path)

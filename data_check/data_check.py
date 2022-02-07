from pathlib import Path
from typing import List, Dict, Any, Union, Optional

from data_check.checks.base_check import BaseCheck

from .config import DataCheckConfig
from .result import DataCheckResult
from .output import DataCheckOutput
from .io import expand_files, read_sql_file, read_yaml, parse_template
from .sql import DataCheckSql
from .runner import DataCheckRunner
from .checks import (
    DataCheckGenerator,
    CSVCheck,
    PipelineCheck,
    EmptySetCheck,
    ExcelCheck,
    PathNotExists,
    TableCheck,
)
from .utils.lookup_loader import load_lookups_from_path


class DataCheck:
    """
    Main class for data_check.
    """

    def __init__(self, config: Optional[DataCheckConfig] = None):
        if config is None:
            config = DataCheckConfig()
        self.config = config
        self.output = DataCheckOutput()
        self.runner = DataCheckRunner(config.parallel_workers, output=self.output)
        if not isinstance(config.connection, str):
            raise Exception("connection is not initialized")
        self.sql = DataCheckSql(
            connection=config.connection, runner=self.runner, output=self.output
        )
        self.template_data: Dict[str, Any] = {}
        self.lookup_data: Dict[str, Any] = {}

    @property
    def sql_params(self) -> Dict[str, Any]:
        return self.lookup_data

    def load_template(self):
        if self.config.template_path.exists():
            self.template_data = read_yaml(self.config.template_path)

    def delegate_test(self, check: BaseCheck) -> DataCheckResult:
        return check.run_test()

    def get_check(self, check_path: Path) -> Optional[BaseCheck]:
        if PathNotExists.is_check_path(check_path):
            return PathNotExists(self, check_path)
        elif PipelineCheck.is_check_path(check_path):
            return PipelineCheck(self, check_path)
        elif EmptySetCheck.is_check_path(check_path):
            return EmptySetCheck(self, check_path)
        elif ExcelCheck.is_check_path(check_path):
            return ExcelCheck(self, check_path)
        elif CSVCheck.is_check_path(check_path):
            if self.config.generate_mode:
                return DataCheckGenerator(self, check_path)
            else:
                return CSVCheck(self, check_path)
        elif TableCheck.is_check_path(check_path):
            return TableCheck(self, check_path)
        else:
            return None

    def collect_checks(
        self, files: List[Path], base_path: Path = Path(".")
    ) -> List[BaseCheck]:
        base_path = base_path.absolute()
        checks: List[BaseCheck] = []
        for f in sorted(files):
            abs_file = f if f.is_absolute() else base_path / f
            check = self.get_check(abs_file)
            if check is not None:
                checks.append(check)
            elif abs_file.is_dir():
                dir_files = [d for d in abs_file.iterdir()]
                checks.extend(self.collect_checks(dir_files, base_path=base_path))
        return checks

    def run(self, files: List[Path], base_path: Path = Path(".")) -> bool:
        """
        Runs a data_check test for all element in the list.
        Returns True, if all calls returned True, otherwise False.
        """
        all_checks = self.collect_checks(files, base_path=base_path)
        results = self.runner.run_checks(self.delegate_test, all_checks)

        overall_result = all(results)
        if self.config.print_overall_result:
            self.output.pprint_overall_result(overall_result)
        return overall_result

    def run_sql_file(self, file: Path):
        sql_text = read_sql_file(sql_file=file, template_data=self.template_data)
        self.output.print("executing:")
        self.output.print(sql_text)
        return self.sql.run_sql(query=sql_text, params=self.sql_params)

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
        print_query: bool = False,
    ):
        sql_query = parse_template(query, template_data=self.template_data)
        if print_query:
            self.output.print(f"-- {sql_query}")
        return self.sql.run_sql(sql_query, output, self.sql_params, base_path)

    def load_lookups(self):
        self.lookup_data.update(load_lookups_from_path(self))

    def write_check(self, query: str, check_path: Path, base_path: Path = Path(".")):
        output_sql = base_path / check_path
        output_csv = output_sql.with_suffix(".csv")
        exists_failed = False
        if output_csv.exists() and not self.config.force:
            self.output.print(f"{output_csv} already exists")
            exists_failed = True
        if output_sql.exists() and not self.config.force:
            self.output.print(f"{output_sql} already exists")
            exists_failed = True
        if exists_failed:
            return False
        query_result = self.run_sql_query(
            query=query, output=output_csv, base_path=base_path
        )
        output_sql.write_text(query)
        return query_result

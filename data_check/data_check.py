from functools import partial
from pathlib import Path
from typing import Any, Optional, Union, cast

from data_check.checks.base_check import BaseCheck

from .checks import (
    CSVCheck,
    DataCheckGenerator,
    EmptySetCheck,
    ExcelCheck,
    PathNotExists,
    PipelineCheck,
    TableCheck,
)
from .config import DataCheckConfig
from .exceptions import ValidationError
from .file_ops import expand_files, parse_template, read_sql_file, read_yaml
from .output import DataCheckOutput
from .result import DataCheckResult
from .runner import DataCheckRunner
from .sql import get_sql
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
        self.runner = DataCheckRunner(
            config.parallel_workers, output=self.output, use_process=config.use_process
        )
        if not isinstance(config.connection, str):
            raise Exception("connection is not initialized")
        self.sql = get_sql(
            connection=config.connection,
            runner=self.runner,
            output=self.output,
            config=self.config,
        )
        self.template_data: dict[str, Any] = {}
        self.lookup_data: dict[str, Any] = {}

    def __del__(self):
        self.sql.disconnect()

    @property
    def sql_params(self) -> dict[str, Any]:
        return self.lookup_data

    def load_template(self):
        if self.config.template_path.exists():
            self.template_data = read_yaml(self.config.template_path)

    def delegate_test(
        self, check: BaseCheck, do_cleanup: bool = True
    ) -> DataCheckResult:
        result = check.run_test()
        if do_cleanup:
            check.cleanup()
            del check
        return result

    def _get_check_or_generator(self, check_path, check) -> BaseCheck:
        if self.config.generate_mode:
            return DataCheckGenerator(self, check_path, check)
        return check

    def get_check(self, check_path: Path) -> Optional[BaseCheck]:
        check: Optional[BaseCheck] = None
        if PathNotExists.is_check_path(check_path):
            check = PathNotExists(self, check_path)
        elif PipelineCheck.is_check_path(check_path):
            check = PipelineCheck(self, check_path)
        elif EmptySetCheck.is_check_path(check_path):
            check = EmptySetCheck(self, check_path)
        elif ExcelCheck.is_check_path(check_path):
            check = ExcelCheck(self, check_path)
        elif CSVCheck.is_check_path(check_path):
            csv_check = CSVCheck(self, check_path)
            check = self._get_check_or_generator(check_path, csv_check)
        elif TableCheck.is_check_path(check_path):
            table_check = TableCheck(self, check_path)
            check = self._get_check_or_generator(check_path, table_check)
        return check

    def collect_checks(
        self, files: list[Path], base_path: Path = Path()
    ) -> list[BaseCheck]:
        base_path = base_path.absolute()
        checks: list[BaseCheck] = []
        check_paths: set[Path] = set()
        for f in sorted(files):
            abs_file = f if f.is_absolute() else base_path / f
            check = self.get_check(abs_file)
            if check is not None and check.check_path not in check_paths:
                checks.append(check)
                check_paths.add(check.check_path)
            elif abs_file.is_dir():
                dir_files = list(abs_file.iterdir())
                checks.extend(self.collect_checks(dir_files, base_path=base_path))
        return checks

    def validate_checks(self, checks: list[BaseCheck]):
        for c in checks:
            try:
                if not c.validate():
                    raise ValidationError(f"{c} validation failed", check=c)
            except Exception as e:
                raise ValidationError(
                    f"{c} validation failed: {e}", check=c, original_exception=e
                ) from e

    def run(
        self, files: list[Path], base_path: Path = Path(), do_cleanup: bool = True
    ) -> bool:
        """
        Runs a data_check test for all element in the list.
        Returns True, if all calls returned True, otherwise False.
        """
        all_checks = self.collect_checks(files, base_path=base_path)
        results = self.run_checks(all_checks, do_cleanup)
        overall_result = self.get_overall_result(
            results, self.config.print_overall_result
        )
        return overall_result

    def run_checks(self, all_checks: list[BaseCheck], do_cleanup: bool = True):
        try:
            self.validate_checks(all_checks)
        except ValidationError as e:
            result = DataCheckResult(
                passed=False,
                source=cast(BaseCheck, e.check).check_path,
                exception=cast(Exception, e.original_exception),
            )
            self.output.print(result)
            return [result]
        delegate = partial(self.delegate_test, do_cleanup=do_cleanup)
        results = self.runner.run_checks(delegate, all_checks)
        return results

    def get_overall_result(
        self,
        results: list[DataCheckResult],
        print_overall: bool = False,
        print_summary: bool = False,
    ):
        overall_result = all(results)
        if print_summary:
            self.output.pprint_result_summary(results)
        if print_overall:
            self.output.pprint_overall_result(overall_result)
        return overall_result

    def run_sql_file(self, file: Path):
        sql_text = read_sql_file(sql_file=file, template_data=self.template_data)
        self.output.print("executing:")
        self.output.print(sql_text)
        return self.sql.run_sql(query=sql_text, params=self.sql_params)

    def run_sql_files(self, files: list[Path], base_path: Path = Path()):
        parameters = [{"file": f} for f in expand_files(files, base_path=base_path)]
        return all(
            self.runner.run_any(run_method=self.run_sql_file, parameters=parameters)
        )

    def run_sql_query(
        self,
        query: str,
        output: Union[str, Path] = "",
        base_path: Path = Path(),
        print_query: bool = False,
        sort_output: bool = False,
    ):
        sql_query = parse_template(query, template_data=self.template_data)
        if print_query:
            self.output.print(f"-- {sql_query}")
        return self.sql.run_sql(
            sql_query, output, self.sql_params, base_path, sort_output=sort_output
        )

    def load_lookups(self):
        self.lookup_data.update(load_lookups_from_path(self))

    def write_check(self, query: str, check_path: Path, base_path: Path = Path()):
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
            query=query, output=output_csv, base_path=base_path, sort_output=True
        )
        output_sql.write_text(query)
        return query_result

    def fake_data(
        self,
        configs: list[Path],
        output: Path = Path(),
        base_path: Path = Path(),
        force: bool = False,
    ):
        from .fake.fake_data import fake_from_config

        parameters = [
            {
                "config": c,
                "sql": self.sql,
                "output": output,
                "force": force,
                "base_path": base_path,
            }
            for c in expand_files(configs, base_path=base_path, extension=".yml")
        ]
        return all(
            self.runner.run_any(run_method=fake_from_config, parameters=parameters)
        )

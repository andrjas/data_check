from __future__ import annotations

import inspect
from copy import deepcopy
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union, cast

from data_check.sql.load_mode import LoadMode
from data_check.utils.deprecation import deprecated_method

from ...file_ops import read_yaml
from ...result import DataCheckResult
from ..base_check import BaseCheck
from .cmd_step import CmdStep
from .serial_pipeline_steps import SerialPipelineSteps

if TYPE_CHECKING:
    from data_check import DataCheck

DATA_CHECK_PIPELINE_FILE = "data_check_pipeline.yml"

StrOrPathList = Union[str, List[str], Path, List[Path]]


class PipelineCheck(BaseCheck):
    def __init__(self, data_check: DataCheck, check_path: Path) -> None:
        if check_path.name == DATA_CHECK_PIPELINE_FILE:
            check_path = check_path.parent
        super().__init__(data_check, check_path)
        self.pipeline_steps: Dict[str, Dict[str, Any]] = {}
        self.register_pipelines()

    def register_pipelines(self):
        self.register_pipeline_step(
            "load",
            self.load_table_or_tables,
        )
        self.register_pipeline_step(
            "load_table",
            self.deprecated_load_table,
            convert_to_path=["file"],
        )
        self.register_pipeline_step("cmd", self.run_cmd)

        # pipeline checks should run with the same connection, so we do not clean them up
        pipeline_check = partial(self.data_check.run, do_cleanup=False)
        self.register_pipeline_step(
            "check", pipeline_check, convert_to_path_list=["files"]
        )
        # run is an alias for check
        self.register_pipeline_step(
            "run", pipeline_check, convert_to_path_list=["files"]
        )
        self.register_pipeline_step(
            "sql_files", self.deprecated_sql_files, convert_to_path_list=["files"]
        )
        self.register_pipeline_step(
            "sql_file", self.deprecated_sql_files, convert_to_path_list=["files"]
        )
        self.register_pipeline_step("sql", self.sql_query_or_files)
        self.register_pipeline_step("always_run", self.always_run)
        self.register_pipeline_step(
            "fake",
            self.data_check.fake_data,
            convert_to_path_list=["configs"],
            convert_to_path=["output"],
        )

    def load_table_or_tables(
        self,
        files: StrOrPathList = "",
        table: str = "",
        file: Union[str, Path] = "",
        mode: Union[str, LoadMode] = LoadMode.DEFAULT,
        base_path: Path = Path("."),
        load_mode: Union[str, LoadMode, None] = None,
    ):
        if files:
            if file:
                raise Exception("load: cannot use files and file at the same time")
            if table:
                raise Exception("load: cannot use files and table at the same time")
            path_list = self.convert_params_to_path_list(files)
            return self.data_check.sql.table_loader.load_tables_from_files(
                files=path_list, mode=mode, base_path=base_path, load_mode=load_mode
            )

        if table:
            if not file:
                raise ValueError("load: file is missing")
            return self.data_check.sql.table_loader.load_table_from_file(
                table=table,
                file=Path(file),
                mode=mode,
                base_path=base_path,
                load_mode=load_mode,
            )

        if file:
            # use file as alias for files
            files = self.convert_params_to_path_list(file)
            return self.data_check.sql.table_loader.load_tables_from_files(
                files=files, mode=mode, base_path=base_path, load_mode=load_mode
            )

        raise ValueError(
            f"""unsupported parameter for load:
{files=}
{table=}
{file=}
{mode=}
"""
        )

    def deprecated_load_table(
        self,
        table: str,
        file: Path,
        mode: Union[str, LoadMode] = LoadMode.DEFAULT,
        base_path: Path = Path("."),
        load_mode: Union[str, LoadMode, None] = None,
    ):
        deprecated_method(
            "load_table",
            """
load:
    table: ...
    file: ...
""",
        )
        return self.data_check.sql.table_loader.load_table_from_file(
            table=table, file=file, mode=mode, base_path=base_path, load_mode=load_mode
        )

    def deprecated_sql_files(self, files: List[Path], base_path: Path = Path(".")):
        deprecated_method(
            "sql_files",
            """
sql:
    files:
""",
        )
        return self.data_check.run_sql_files(files=files, base_path=base_path)

    def sql_query_or_files(
        self,
        query_or_files: StrOrPathList = "",
        query: str = "",
        output: Union[str, Path] = "",
        print_query: bool = True,
        files: List[Path] = [],
        file: List[Path] = [],
        base_path: Path = Path("."),
        write_check: Union[str, Path] = "",
    ):
        if base_path == Path("."):
            base_path = self.check_path

        if query_or_files and not query:
            if isinstance(query_or_files, str):
                try_file = base_path / query_or_files
                if try_file.exists():
                    files = self.convert_params_to_path_list(query_or_files)
                else:
                    query = query_or_files
            elif isinstance(query_or_files, List):
                path_list = self.convert_params_to_path_list(query_or_files)
                files = path_list

        # file is an alias for files
        if file:
            if files:
                raise Exception(f"cannot use files and file at the same time")
            files = file

        if files:
            path_list = self.convert_params_to_path_list(files)
            return self.data_check.run_sql_files(files=path_list, base_path=base_path)

        if query:
            if write_check:
                return self.data_check.write_check(
                    query=query, check_path=Path(write_check), base_path=base_path
                )
            return self.data_check.run_sql_query(
                query=query, output=output, base_path=base_path, print_query=print_query
            )

        raise ValueError(
            f"""unsupported parameter for sql:
{query_or_files=}
{query=}
{output=}
{files=}
"""
        )

    @staticmethod
    def convert_params_to_path_list(params: StrOrPathList) -> List[Path]:
        if isinstance(params, str):
            return [Path(params)]
        elif isinstance(params, List):
            return [Path(p) for p in params]
        elif isinstance(params, Path):
            return [Path(params)]
        else:
            raise ValueError(f"unsupported type {type(params)} for {params}")

    @staticmethod
    def is_check_path(path: Path) -> bool:
        return (path.is_dir() and (path / DATA_CHECK_PIPELINE_FILE).exists()) or (
            path.name == DATA_CHECK_PIPELINE_FILE and path.exists()
        )

    def register_pipeline_step(
        self,
        step_name: str,
        method: Callable[..., Any],
        convert_to_path_list: Optional[List[str]] = [],
        convert_to_path: Optional[List[str]] = [],
    ):
        self.pipeline_steps[step_name] = {
            "method": method,
            "convert_to_path_list": convert_to_path_list,
            "convert_to_path": convert_to_path,
        }

    def get_pipeline_method(self, step_name: str) -> Optional[Callable[..., Any]]:
        return self.pipeline_steps.get(step_name, {}).get("method", None)

    def _parse_pipeline_file(self, pipeline_file: Path) -> Dict[str, Any]:
        if pipeline_file.exists():
            yaml = read_yaml(
                pipeline_file,
                template_data=dict(
                    self.template_parameters(pipeline_file.parent),
                    **self.data_check.template_data,
                ),
            )
            return yaml or {}  # yaml can return None, so convert it to an empty dict
        return {}

    def _get_steps(self, path: Path) -> List[Any]:
        pipeline_config = self._parse_pipeline_file(path / DATA_CHECK_PIPELINE_FILE)
        steps = pipeline_config.get("steps", [])
        return steps or []

    def run_steps_pipeline(self, steps: List[Any]) -> DataCheckResult:
        serial_steps = SerialPipelineSteps(
            self.data_check,
            self,
            steps,
            self.check_path,
            pipeline_name=str(self.check_path),
        )
        return serial_steps.run()

    def run_test(self) -> DataCheckResult:
        steps = self._get_steps(self.check_path)
        return self.run_steps_pipeline(steps)

    def run_cmd(
        self, commands: List[str], print: bool = True, base_path: Path = Path(".")
    ):
        c = CmdStep(commands, self.data_check.output, print=print)
        return c.run(base_path=base_path)

    def always_run(self, steps: List[Any]) -> DataCheckResult:
        serial_steps = SerialPipelineSteps(
            self.data_check,
            self,
            steps,
            self.check_path,
            pipeline_name=str(self.check_path),
        )
        # Also return the result of the run,
        # so that if all steps pass,
        # the overall result will be also passed.
        return serial_steps.run()

    def template_parameters(self, pipeline_path: Path) -> Dict[str, str]:
        return {
            "CONNECTION": cast(str, self.data_check.config.connection_name),
            "CONNECTION_STRING": cast(str, self.data_check.config.connection),
            "PIPELINE_PATH": str(pipeline_path.absolute()),
            "PIPELINE_NAME": str(pipeline_path.name),
            "PROJECT_PATH": str(self.data_check.config.project_path),
        }

    def get_prepared_parameters(
        self, method: str, params: Union[str, List[str], Dict[str, Any]]
    ):
        prepared_params: Dict[str, Any] = {}
        if isinstance(params, str):
            # If only a string is given, convert it to a list of the first
            # convert_to_path_list argument
            # or, if no path list given, to the first convert_to_path argument.
            try:
                first_param = self.pipeline_steps.get(method, {}).get(
                    "convert_to_path_list", []
                )[0]
                prepared_params[first_param] = [Path(params)]
            except IndexError:
                try:
                    first_param = self.pipeline_steps.get(method, {}).get(
                        "convert_to_path", []
                    )[0]
                    prepared_params[first_param] = Path(params)
                except IndexError:
                    # if this didn't work, try to get the first argument from inspection
                    _method = self.pipeline_steps.get(method, {}).get("method")
                    argspec = inspect.getfullargspec(_method)
                    first_param = (
                        argspec.args[1]
                        if argspec.args[0] == "self"
                        else argspec.args[0]
                    )
                    prepared_params[first_param] = params

        elif isinstance(params, List):
            # if list (of strings) is given, convert it to a list of the first
            # convert_to_path_list argument
            try:
                first_param = self.pipeline_steps.get(method, {}).get(
                    "convert_to_path_list", []
                )[0]
                prepared_params[first_param] = [Path(p) for p in params]
            except IndexError:
                # if this didn't work, pass the list to
                # the first argument from inspection
                _method = self.pipeline_steps.get(method, {}).get("method")
                argspec = inspect.getfullargspec(_method)
                first_param = (
                    argspec.args[1] if argspec.args[0] == "self" else argspec.args[0]
                )
                prepared_params[first_param] = params
        elif isinstance(params, dict):
            param_list = self.pipeline_steps.get(method, {}).get(
                "convert_to_path_list", []
            )
            prepared_params = deepcopy(params)
            for _param in param_list:
                pars = params[_param]
                if isinstance(pars, list):
                    prepared_params[_param] = [Path(p) for p in pars]
                elif isinstance(pars, str):
                    prepared_params[_param] = [Path(pars)]
                else:
                    raise Exception(f"unexpected parameter type: {pars}")
            param_list_path = self.pipeline_steps.get(method, {}).get(
                "convert_to_path", []
            )
            for _param in param_list_path:
                prepared_params[_param] = Path(params[_param])
        return prepared_params

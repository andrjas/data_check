from __future__ import annotations
from pathlib import Path
from typing import Callable, List, Dict, Any, Union, Optional, TYPE_CHECKING, cast
from copy import deepcopy
import inspect
from functools import partial

from ..base_check import BaseCheck
from ...result import DataCheckResult
from ...io import read_yaml
from .serial_pipeline_steps import SerialPipelineSteps
from .cmd_step import CmdStep

if TYPE_CHECKING:
    from data_check import DataCheck

DATA_CHECK_PIPELINE_FILE = "data_check_pipeline.yml"


class PipelineCheck(BaseCheck):
    def __init__(self, data_check: DataCheck, check_path: Path) -> None:
        super().__init__(data_check, check_path)
        self.pipeline_steps: Dict[str, Dict[str, Any]] = {}
        self.register_pipelines()

    def register_pipelines(self):
        self.register_pipeline_step(
            "load_tables",
            self.data_check.sql.table_loader.load_tables_from_files,
            convert_to_path_list=["files"],
        )
        self.register_pipeline_step(
            "load",
            self.data_check.sql.table_loader.load_table_from_file,
            convert_to_path=["file"],
        )
        self.register_pipeline_step("cmd", self.run_cmd)
        self.register_pipeline_step(
            "check", self.data_check.run, convert_to_path_list=["files"]
        )
        self.register_pipeline_step(
            "sql_files", self.data_check.run_sql_files, convert_to_path_list=["files"]
        )
        self.register_pipeline_step(
            "sql_file", self.data_check.run_sql_files, convert_to_path_list=["files"]
        )
        pipeline_run_sql_query = partial(
            self.data_check.run_sql_query, print_query=True
        )
        self.register_pipeline_step("sql", pipeline_run_sql_query)
        self.register_pipeline_step("always_run", self.always_run)

    @staticmethod
    def is_check_path(path: Path) -> bool:
        return path.is_dir() and (path / DATA_CHECK_PIPELINE_FILE).exists()

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
            return (
                yaml if yaml else {}
            )  # yaml can return None, so convert it to an empty dict
        else:
            return {}

    def _get_steps(self, path: Path) -> List[Any]:
        pipeline_config = self._parse_pipeline_file(path / DATA_CHECK_PIPELINE_FILE)
        steps = pipeline_config.get("steps", [])
        return steps if steps else []

    def run_test(self) -> DataCheckResult:
        steps = self._get_steps(self.check_path)
        serial_steps = SerialPipelineSteps(
            self.data_check,
            self,
            steps,
            self.check_path,
            pipeline_name=str(self.check_path),
        )
        return serial_steps.run()

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
        prepared_params = dict()
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

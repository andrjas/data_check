from pathlib import Path
from typing import Callable, List, Dict, Any, Union, Optional
from copy import deepcopy
import inspect
import subprocess

from ..result import DataCheckResult, ResultType
from ..io import read_yaml, parse_template

DATA_CHECK_PIPELINE_FILE = "data_check_pipeline.yml"


class SerialPipelineSteps:
    def __init__(
        self, data_check, steps: List[Any], path: Path, pipeline_name: str
    ) -> None:
        self.data_check = data_check
        self.path = path
        self.steps = steps
        self.pipeline_name = pipeline_name

    def run(self) -> DataCheckResult:
        for step in self.steps:
            try:
                result = self.run_pipeline_step(self.path, step)
                if not result:
                    result_msg = (
                        f"pipeline {self.pipeline_name}: "
                        f"{self.data_check.output.failed_message}"
                    )
                    return DataCheckResult(
                        passed=False,
                        result=result,
                        message=result_msg,
                    )
            except Exception as e:
                return self.data_check.output.prepare_result(
                    result_type=ResultType.FAILED_WITH_EXCEPTION,
                    source=self.path,
                    exception=e,
                )

        return DataCheckResult(
            passed=True,
            result=(
                f"pipeline {self.pipeline_name}: "
                f"{self.data_check.output.passed_message}"
            ),
        )

    def run_pipeline_step(self, path: Path, step: Dict[str, Any]):
        step_type = next(iter(step.keys()))
        params = next(iter(step.values()))
        call_method = self.data_check.get_pipeline_method(step_type)
        if call_method:
            prepared_params = self.data_check.get_prepared_parameters(step_type, params)
            argspec = inspect.getfullargspec(call_method)
            if "base_path" in argspec.args:
                prepared_params.update({"base_path": path})
            return call_method(**prepared_params)
        else:
            raise Exception(f"unknown pipeline step: {step_type}")


class CmdStep:
    def __init__(self, cmd: Union[str, List[str]]) -> None:
        self.cmd = cmd

    def _run_cmd(self, cmd: str, base_path: Path = Path(".")):
        print(f"# {cmd}")
        try:
            subprocess.run(args=cmd, cwd=base_path, shell=True, check=True)
            return True
        except subprocess.CalledProcessError:
            return False

    def run(self, base_path: Path = Path(".")):
        # print(f"run {self.cmd}")
        if isinstance(self.cmd, List):
            for c in self.cmd:
                if not self._run_cmd(c, base_path=base_path):
                    return False
        elif isinstance(self.cmd, str):
            return self._run_cmd(self.cmd, base_path=base_path)
        else:
            raise Exception(f"unknown cmd type: {self.cmd}")
        return True


class PipelineCheck:
    def __init__(self) -> None:
        self.pipeline_steps = {}

    def register_pipelines(self):
        self.register_pipeline_step(
            "load_tables",
            self.sql.load_tables_from_files,
            convert_to_path_list=["files"],
        )
        self.register_pipeline_step(
            "load", self.sql.load_table_from_csv_file, convert_to_path=["file"]
        )
        self.register_pipeline_step("cmd", self.run_cmd)
        self.register_pipeline_step("check", self.run, convert_to_path_list=["files"])
        self.register_pipeline_step(
            "run_sql", self.run_sql_files, convert_to_path_list=["files"]
        )
        self.register_pipeline_step("sql", self.sql.run_sql)

    @staticmethod
    def is_pipeline_check(path: Path) -> bool:
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

    def get_pipeline_method(self, step_name):
        return self.pipeline_steps.get(step_name, {}).get("method", None)

    def _parse_pipeline_file(self, pipeline_file: Path) -> Dict[str, Any]:
        if pipeline_file.exists():
            yaml = read_yaml(
                pipeline_file,
                template_data=self.template_parameters(pipeline_file.parent),
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

    def run_pipeline(self, path: Path) -> DataCheckResult:
        steps = self._get_steps(path)
        serial_steps = SerialPipelineSteps(self, steps, path, pipeline_name=str(path))
        return serial_steps.run()

    def run_cmd(self, commands: List[str], base_path: Path = Path(".")):
        c = CmdStep(commands)
        return c.run(base_path=base_path)

    def template_parameters(self, pipeline_path: Path) -> Dict[str, str]:
        return {
            "CONNECTION": self.config.connection_name,
            "CONNECTION_STRING": self.config.connection,
            "PIPELINE_PATH": str(pipeline_path),
            "PROJECT_PATH": str(self.config.project_path),
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

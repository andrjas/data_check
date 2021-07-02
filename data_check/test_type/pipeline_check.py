from pathlib import Path
from typing import Callable, List, Tuple, Dict, Any, Union, Optional
from copy import deepcopy
import inspect

from ..result import DataCheckResult
from ..io import read_yaml

DATA_CHECK_PIPELINE_FILE = "data_check_pipeline.yml"


class SerialPipelineSteps:
    def __init__(self, data_check, steps: List[Any], path: Path) -> None:
        self.data_check = data_check
        self.path = path
        self.steps = steps

    def run(self):
        for step in self.steps:
            self.run_pipeline_step(self.path, step)

    def run_pipeline_step(self, path: Path, step: Dict[str, Any]):
        step_type = next(iter(step.keys()))
        params = next(iter(step.values()))
        print(step_type, params)
        call_method = self.data_check.get_pipeline_method(step_type)
        if call_method:
            prepared_params = self.data_check.get_prepared_parameters(path, step_type, params)
            # return self._call_method(call_method, prepared_params)
            print(f"call {call_method} with {prepared_params}")
            return call_method(**prepared_params)
        else:
            raise Exception(f"unknown pipeline step: {step_type}")


class CmdStep:
    def __init__(self, cmd) -> None:
        self.cmd = cmd

    def run(self):
        pass


class PipelineCheck:
    def __init__(self) -> None:
        self.pipeline_steps = {}

    def register_pipelines(self):
        # TODO: move register_pipeline_step to each component
        self.register_pipeline_step("load_tables", self.sql.load_tables_from_files_str, convert_to_path_list=["files"])
        self.register_pipeline_step("load", self.sql.load_table_from_csv_file_str, convert_to_path=["csv_file"])
        self.register_pipeline_step("cmd", self.run_cmd)
        self.register_pipeline_step("check", self.run, convert_to_path_list=["files"])
        self.register_pipeline_step("run_sql", self.run_sql_files, convert_to_path_list=["sql_files"])

    @staticmethod
    def is_pipeline_check(path: Path) -> bool:
        return path.is_dir() and (path / DATA_CHECK_PIPELINE_FILE).exists()

    def register_pipeline_step(self, step_name: str, method: Callable[..., Any], convert_to_path_list: Optional[List[str]] = [], convert_to_path: Optional[List[str]] = []):
        self.pipeline_steps[step_name] = {
            "method": method,
            "convert_to_path_list": convert_to_path_list,
            "convert_to_path": convert_to_path,
        }

    def get_pipeline_method(self, step_name):
        return self.pipeline_steps.get(step_name, {}).get("method", None)

    def _parse_pipeline_file(self, pipeline_file: Path) -> Dict[str, Any]:
        return read_yaml(pipeline_file)

    def _get_steps(self, path: Path) -> List[Any]:
        pipeline_config = self._parse_pipeline_file(path / DATA_CHECK_PIPELINE_FILE)
        return pipeline_config.get("steps", [])

    def run_pipeline(self, path: Path) -> DataCheckResult:
        steps = self._get_steps(path)
        serial_steps = SerialPipelineSteps(self, steps, path)
        serial_steps.run()

    def run_cmd(self, cmd: List[str]):
        c = CmdStep(cmd)
        return c.run()

    def get_prepared_parameters(self, path: Path, method: str, params: Union[str, List[str], Dict[str, Any]]):
        prepared_params = dict()
        if isinstance(params, str):
            # If only a string is given, convert it to a list of the first convert_to_path_list argument
            # or, if no path list given, to the first convert_to_path argument.
            try:
                first_param = self.pipeline_steps.get(method, {}).get("convert_to_path_list")[0]
                prepared_params[first_param] = [path / Path(params)]
            except IndexError:
                try:
                    first_param = self.pipeline_steps.get(method, {}).get("convert_to_path")[0]
                    prepared_params[first_param] = path / Path(params)
                except IndexError:
                    # if this didn't work, try to get the first argument from inspection
                    method = self.pipeline_steps.get(method, {}).get("method")
                    argspec = inspect.getfullargspec(method)
                    first_param = argspec.args[1] if argspec.args[0] == 'self' else argspec.args[0]
                    prepared_params[first_param] = params

        elif isinstance(params, List):
            # if list (of strings) is given, convert it to a list of the first convert_to_path_list argument
            first_param = self.pipeline_steps.get(method, {}).get("convert_to_path_list")[0]
            prepared_params[first_param] = [(path / Path(p)) for p in params]
        elif isinstance(params, dict):
            param_list = self.pipeline_steps.get(method, {}).get("convert_to_path_list")
            prepared_params = deepcopy(params)
            for _param in param_list:
                prepared_params[_param] = [(path / Path(p)) for p in params[_param]]
        return prepared_params

    # def _call_method(self, method, params):
    #     if isinstance(params, list):
    #         return method(*params)
    #     else:
    #         return method(**params)

    # def run_pipeline_step(self, path: Path, step: Dict[str, Any]):
    #     step_type = next(iter(step.keys()))
    #     params = next(iter(step.values()))
    #     print(step_type, params)
    #     methods = {
    #         "load_tables": self.sql.load_tables_from_files_str,
    #         "load": self.sql.load_table_from_csv_file_str,
    #         "cmd": None,
    #         "check": self.run,
    #         "run_sql": self.run_sql_files,
    #     }
    #     prepared_params = self._prepare_parameter(step_type, params)
    #     call_method = methods.get(step_type, None)
    #     if call_method:
    #         return self._call_method(call_method, prepared_params)
    #     else:
    #         raise Exception(f"unknown pipeline step: {step_type}")


import sys
import os
import pytest
from pathlib import Path


# add the parent path to PYTHONPATH
my_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, my_path + "/../")

from data_check import DataCheck  # noqa E402
from data_check.config import DataCheckConfig  # noqa E402
from data_check.result import DataCheckResult  # noqa E402


@pytest.fixture
def dc() -> DataCheck:
    config = DataCheckConfig().load_config().set_connection("test")
    config.parallel_workers = 1  # since we do not persist anything in SQLite, we must a single connection
    _dc = DataCheck(config)
    _dc.load_template()
    return _dc


def test_is_pipeline_check(dc: DataCheck):
    assert dc.is_pipeline_check(Path("checks/pipelines/simple_pipeline"))


def test_simple_pipeline(dc: DataCheck):
    result = dc.run_pipeline(Path("checks/pipelines/simple_pipeline"))
    assert result


def test_pipeline_sqls_are_not_in_simple_tests(dc: DataCheck):
    checks = dc.collect_checks([Path("checks")])
    pipeline_test_sql = Path("checks/pipelines/simple_pipeline/pipeline_checks/test_simple_pipeline.sql")
    assert pipeline_test_sql.exists()
    assert pipeline_test_sql not in checks


def test_register_pipeline_step(dc: DataCheck):
    def step_test():
        pass
    dc.register_pipeline_step("step_test", step_test)
    method = dc.get_pipeline_method("step_test")
    assert method == step_test


def test_prepared_parameters_str_to_path_list(dc: DataCheck):
    def step_test(arg1):
        pass
    dc.register_pipeline_step("step_test", step_test, ['arg1'])
    parameters = dc.get_prepared_parameters( "step_test", "test_arg1")
    assert parameters == {"arg1": [Path("test_arg1")]}


def test_prepared_parameters_str_list_to_path_list(dc: DataCheck):
    def step_test(arg1):
        pass
    dc.register_pipeline_step("step_test", step_test, ['arg1'])
    parameters = dc.get_prepared_parameters("step_test", ["test_arg1"])
    assert parameters == {"arg1": [Path("test_arg1")]}


def test_prepared_parameters_dict_to_path_list(dc: DataCheck):
    def step_test(arg1):
        pass
    dc.register_pipeline_step("step_test", step_test, ['arg1'])
    parameters = dc.get_prepared_parameters("step_test", {"arg1": ["test_arg1"]})
    assert parameters == {"arg1": [Path("test_arg1")]}


def test_prepared_parameters_str_to_path(dc: DataCheck):
    def step_test(arg1):
        pass
    dc.register_pipeline_step("step_test", step_test, convert_to_path=['arg1'])
    parameters = dc.get_prepared_parameters("step_test", "test_arg1")
    assert parameters == {"arg1": Path("test_arg1")}


def test_prepared_parameters_no_convert(dc: DataCheck):
    def step_test(arg1):
        pass
    dc.register_pipeline_step("step_test", step_test)
    parameters = dc.get_prepared_parameters("step_test", "test_arg1")
    assert parameters == {"arg1": "test_arg1"}


def test_run_pipeline_returns_DataCheckResult(dc: DataCheck):
    result = dc.run_pipeline(Path("checks/pipelines/simple_pipeline"))
    assert isinstance(result, DataCheckResult)

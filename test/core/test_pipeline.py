import pytest
from pathlib import Path


from data_check import DataCheck  # noqa E402
from data_check.config import DataCheckConfig  # noqa E402
from data_check.result import DataCheckResult  # noqa E402
from data_check.checks.pipeline_check import PipelineCheck  # noqa E402


@pytest.fixture
def dc() -> DataCheck:
    config = DataCheckConfig().load_config().set_connection("test")
    config.parallel_workers = (
        1  # since we do not persist anything in SQLite, we must use a single connection
    )
    _dc = DataCheck(config)
    _dc.load_template()
    return _dc


@pytest.fixture
def pc(dc: DataCheck) -> PipelineCheck:
    _pc = PipelineCheck(dc, None)
    return _pc


def test_is_pipeline_check():
    assert PipelineCheck.is_check_path(Path("checks/pipelines/simple_pipeline"))


def test_simple_pipeline(pc: PipelineCheck):
    pc.check_path = Path("checks/pipelines/simple_pipeline")
    result = pc.run_test()
    print(result)
    assert result


def test_pipeline_sqls_are_not_in_simple_tests(dc: DataCheck):
    checks = dc.collect_checks([Path("checks")])
    pipeline_test_sql = Path(
        "checks/pipelines/simple_pipeline/pipeline_checks/test_simple_pipeline.sql"
    )
    assert pipeline_test_sql.exists()
    assert pipeline_test_sql not in checks


def test_register_pipeline_step(pc: PipelineCheck):
    def step_test():
        pass

    pc.register_pipeline_step("step_test", step_test)
    method = pc.get_pipeline_method("step_test")
    assert method == step_test


def test_prepared_parameters_str_to_path_list(pc: PipelineCheck):
    def step_test(arg1):
        pass

    pc.register_pipeline_step("step_test", step_test, ["arg1"])
    parameters = pc.get_prepared_parameters("step_test", "test_arg1")
    assert parameters == {"arg1": [Path("test_arg1")]}


def test_prepared_parameters_str_list_to_path_list(pc: PipelineCheck):
    def step_test(arg1):
        pass

    pc.register_pipeline_step("step_test", step_test, ["arg1"])
    parameters = pc.get_prepared_parameters("step_test", ["test_arg1"])
    assert parameters == {"arg1": [Path("test_arg1")]}


def test_prepared_parameters_dict_to_path_list(pc: PipelineCheck):
    def step_test(arg1):
        pass

    pc.register_pipeline_step("step_test", step_test, ["arg1"])
    parameters = pc.get_prepared_parameters("step_test", {"arg1": ["test_arg1"]})
    assert parameters == {"arg1": [Path("test_arg1")]}


def test_prepared_parameters_dict_str_to_path_list(pc: PipelineCheck):
    def step_test(arg1):
        pass

    pc.register_pipeline_step("step_test", step_test, ["arg1"])
    parameters = pc.get_prepared_parameters("step_test", {"arg1": "test_arg1"})
    assert parameters == {"arg1": [Path("test_arg1")]}


def test_prepared_parameters_str_to_path(pc: PipelineCheck):
    def step_test(arg1):
        pass

    pc.register_pipeline_step("step_test", step_test, convert_to_path=["arg1"])
    parameters = pc.get_prepared_parameters("step_test", "test_arg1")
    assert parameters == {"arg1": Path("test_arg1")}


def test_prepared_parameters_no_convert(pc: PipelineCheck):
    def step_test(arg1):
        pass

    pc.register_pipeline_step("step_test", step_test)
    parameters = pc.get_prepared_parameters("step_test", "test_arg1")
    assert parameters == {"arg1": "test_arg1"}


def test_run_test_returns_DataCheckResult(pc: PipelineCheck):
    pc.check_path = Path("checks/pipelines/simple_pipeline")
    result = pc.run_test()
    assert isinstance(result, DataCheckResult)


def test_pipeline_fails_load_no_csv_file(pc: PipelineCheck):
    pc.check_path = Path("checks/pipelines/failing/load_no_csv_file")
    result = pc.run_test()
    assert not result


def test_pipeline_fails_load_invalid_load_mode(pc: PipelineCheck):
    pc.check_path = Path("checks/pipelines/failing/load_invalid_load_mode")
    result = pc.run_test()
    assert not result
    assert "with exception" in result.message


def test_pipeline_fails_load_tables_no_csv_file(pc: PipelineCheck):
    pc.check_path = Path("checks/pipelines/failing/load_tables_no_csv_file")
    result = pc.run_test()
    assert not result
    assert "with exception" in result.message


def test_pipeline_fails_load_tables_invalid_load_mode(pc: PipelineCheck):
    pc.check_path = Path("checks/pipelines/failing/load_tables_invalid_load_mode")
    result = pc.run_test()
    assert not result
    assert "with exception" in result.message


def test_pipeline_fails_invalid_cmd(pc: PipelineCheck):
    pc.check_path = Path("checks/pipelines/failing/invalid_cmd")
    result = pc.run_test()
    assert not result


def test_pipeline_fails_run_sql_no_file(pc: PipelineCheck):
    pc.check_path = Path("checks/pipelines/failing/run_sql_no_file")
    result = pc.run_test()
    assert not result
    assert "with exception" in result.message


def test_pipeline_fails_run_sql_invalid_query(pc: PipelineCheck):
    pc.check_path = Path("checks/pipelines/failing/run_sql_invalid_query")
    result = pc.run_test()
    assert not result
    assert "with exception" in result.message


def test_pipeline_empty_pipeline(pc: PipelineCheck):
    pc.check_path = Path("checks/pipelines/empty_pipeline")
    result = pc.run_test()
    assert result


def test_pipeline_no_steps(pc: PipelineCheck):
    pc.check_path = Path("checks/pipelines/no_steps")
    result = pc.run_test()
    assert result


def test_pipeline_failing_nested_pipeline_fails(pc: PipelineCheck):
    pc.check_path = Path("checks/pipelines/failing/nested_pipeline")
    result = pc.run_test()
    assert not result


def test_pipeline_generate(pc: PipelineCheck, tmp_path: Path):
    p1 = tmp_path / "p1"
    p1.mkdir()
    dcp = p1 / "data_check_pipeline.yml"
    dcp.write_text(
        """
steps:
    - check: c1
"""
    )
    c1 = p1 / "c1"
    c1.mkdir()
    check = c1 / "check.sql"
    check.write_text(
        """
select 1 as a
"""
    )
    pc.data_check.config.generate_mode = True
    pc.check_path = p1
    result = pc.run_test()
    print(result)
    assert result
    check_csv = c1 / "check.csv"
    assert check_csv.exists()


def test_pipeline_generate_force(pc: PipelineCheck, tmp_path: Path):
    p1 = tmp_path / "p1"
    p1.mkdir()
    dcp = p1 / "data_check_pipeline.yml"
    dcp.write_text(
        """
steps:
    - check: c1
"""
    )
    c1 = p1 / "c1"
    c1.mkdir()
    check = c1 / "check.sql"
    check.write_text(
        """
select 1 as a
"""
    )
    check_csv = c1 / "check.csv"
    check_csv.write_text("test")
    pc.data_check.config.generate_mode = True
    pc.data_check.config.force = True
    pc.check_path = p1
    result = pc.run_test()
    assert result
    assert check_csv.exists()
    assert check_csv.read_text() == "a\n1\n"


def test_pipeline_path_is_absolute_path(pc: PipelineCheck):
    tp = pc.template_parameters(pipeline_path=Path("checks/pipelines/simple_pipeline"))
    pipeline_path = Path(tp["PIPELINE_PATH"])
    assert pipeline_path.is_absolute()
    assert pipeline_path == Path("checks/pipelines/simple_pipeline").absolute()


def test_pipeline_name(pc: PipelineCheck):
    tp = pc.template_parameters(pipeline_path=Path("checks/pipelines/simple_pipeline"))
    pipeline_name = tp["PIPELINE_NAME"]
    assert pipeline_name == "simple_pipeline"


def test_project_path_is_absolute_path(pc: PipelineCheck):
    tp = pc.template_parameters(pipeline_path=Path("checks/pipelines/simple_pipeline"))
    project_path = Path(tp["PROJECT_PATH"])
    assert project_path.is_absolute()
    assert project_path == Path(".").absolute()


def test_pipeline_template_connection(pc: PipelineCheck):
    tp = pc.template_parameters(pipeline_path=Path("checks/pipelines/simple_pipeline"))
    connection = tp["CONNECTION"]
    assert connection == "test"


def test_pipeline_template_connection_string(pc: PipelineCheck):
    tp = pc.template_parameters(pipeline_path=Path("checks/pipelines/simple_pipeline"))
    connection_string = tp["CONNECTION_STRING"]
    assert connection_string == "sqlite+pysqlite://"
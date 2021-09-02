import pytest
from pathlib import Path


from data_check import DataCheck  # noqa E402
from data_check.config import DataCheckConfig  # noqa E402
from data_check.result import DataCheckResult  # noqa E402


@pytest.fixture
def dc() -> DataCheck:
    config = DataCheckConfig().load_config().set_connection("test")
    config.parallel_workers = (
        1  # since we do not persist anything in SQLite, we must use a single connection
    )
    _dc = DataCheck(config)
    _dc.load_template()
    return _dc


def test_is_pipeline_check(dc: DataCheck):
    assert dc.is_pipeline_check(Path("checks/pipelines/simple_pipeline"))


def test_simple_pipeline(dc: DataCheck):
    result = dc.run_pipeline(Path("checks/pipelines/simple_pipeline"))
    print(result)
    assert result


def test_pipeline_sqls_are_not_in_simple_tests(dc: DataCheck):
    checks = dc.collect_checks([Path("checks")])
    pipeline_test_sql = Path(
        "checks/pipelines/simple_pipeline/pipeline_checks/test_simple_pipeline.sql"
    )
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

    dc.register_pipeline_step("step_test", step_test, ["arg1"])
    parameters = dc.get_prepared_parameters("step_test", "test_arg1")
    assert parameters == {"arg1": [Path("test_arg1")]}


def test_prepared_parameters_str_list_to_path_list(dc: DataCheck):
    def step_test(arg1):
        pass

    dc.register_pipeline_step("step_test", step_test, ["arg1"])
    parameters = dc.get_prepared_parameters("step_test", ["test_arg1"])
    assert parameters == {"arg1": [Path("test_arg1")]}


def test_prepared_parameters_dict_to_path_list(dc: DataCheck):
    def step_test(arg1):
        pass

    dc.register_pipeline_step("step_test", step_test, ["arg1"])
    parameters = dc.get_prepared_parameters("step_test", {"arg1": ["test_arg1"]})
    assert parameters == {"arg1": [Path("test_arg1")]}


def test_prepared_parameters_dict_str_to_path_list(dc: DataCheck):
    def step_test(arg1):
        pass

    dc.register_pipeline_step("step_test", step_test, ["arg1"])
    parameters = dc.get_prepared_parameters("step_test", {"arg1": "test_arg1"})
    assert parameters == {"arg1": [Path("test_arg1")]}


def test_prepared_parameters_str_to_path(dc: DataCheck):
    def step_test(arg1):
        pass

    dc.register_pipeline_step("step_test", step_test, convert_to_path=["arg1"])
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


def test_pipeline_fails_load_no_csv_file(dc: DataCheck):
    result = dc.run_pipeline(Path("checks/pipelines/failing/load_no_csv_file"))
    assert not result


def test_pipeline_fails_load_invalid_load_mode(dc: DataCheck):
    result = dc.run_pipeline(Path("checks/pipelines/failing/load_invalid_load_mode"))
    assert not result
    assert "with exception" in result.message


def test_pipeline_fails_load_tables_no_csv_file(dc: DataCheck):
    result = dc.run_pipeline(Path("checks/pipelines/failing/load_tables_no_csv_file"))
    assert not result
    assert "with exception" in result.message


def test_pipeline_fails_load_tables_invalid_load_mode(dc: DataCheck):
    result = dc.run_pipeline(
        Path("checks/pipelines/failing/load_tables_invalid_load_mode")
    )
    assert not result
    assert "with exception" in result.message


def test_pipeline_fails_invalid_cmd(dc: DataCheck):
    result = dc.run_pipeline(Path("checks/pipelines/failing/invalid_cmd"))
    assert not result


def test_pipeline_fails_run_sql_no_file(dc: DataCheck):
    result = dc.run_pipeline(Path("checks/pipelines/failing/run_sql_no_file"))
    assert not result
    assert "with exception" in result.message


def test_pipeline_fails_run_sql_invalid_query(dc: DataCheck):
    result = dc.run_pipeline(Path("checks/pipelines/failing/run_sql_invalid_query"))
    assert not result
    assert "with exception" in result.message


def test_pipeline_empty_pipeline(dc: DataCheck):
    result = dc.run_pipeline(Path("checks/pipelines/empty_pipeline"))
    assert result


def test_pipeline_no_steps(dc: DataCheck):
    result = dc.run_pipeline(Path("checks/pipelines/no_steps"))
    assert result


def test_pipeline_failing_nested_pipeline_fails(dc: DataCheck):
    result = dc.run_pipeline(Path("checks/pipelines/failing/nested_pipeline"))
    assert not result


def test_pipeline_generate(dc: DataCheck, tmp_path: Path):
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
    dc.config.generate_mode = True
    result = dc.run_pipeline(p1)
    assert result
    check_csv = c1 / "check.csv"
    assert check_csv.exists()


def test_pipeline_generate_force(dc: DataCheck, tmp_path: Path):
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
    dc.config.generate_mode = True
    dc.config.force = True
    result = dc.run_pipeline(p1)
    assert result
    assert check_csv.exists()
    assert check_csv.read_text() == "a\n1\n"


def test_pipeline_path_is_absolute_path(dc: DataCheck):
    tp = dc.template_parameters(pipeline_path=Path("checks/pipelines/simple_pipeline"))
    pipeline_path = Path(tp["PIPELINE_PATH"])
    assert pipeline_path.is_absolute()
    assert pipeline_path == Path("checks/pipelines/simple_pipeline").absolute()


def test_pipeline_name(dc: DataCheck):
    tp = dc.template_parameters(pipeline_path=Path("checks/pipelines/simple_pipeline"))
    pipeline_name = tp["PIPELINE_NAME"]
    assert pipeline_name == "simple_pipeline"


def test_project_path_is_absolute_path(dc: DataCheck):
    tp = dc.template_parameters(pipeline_path=Path("checks/pipelines/simple_pipeline"))
    project_path = Path(tp["PROJECT_PATH"])
    assert project_path.is_absolute()
    assert project_path == Path(".").absolute()


def test_pipeline_template_connection(dc: DataCheck):
    tp = dc.template_parameters(pipeline_path=Path("checks/pipelines/simple_pipeline"))
    connection = tp["CONNECTION"]
    assert connection == "test"


def test_pipeline_template_connection_string(dc: DataCheck):
    tp = dc.template_parameters(pipeline_path=Path("checks/pipelines/simple_pipeline"))
    connection_string = tp["CONNECTION_STRING"]
    assert connection_string == "sqlite+pysqlite://"

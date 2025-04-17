from pathlib import Path

import pytest

from data_check import DataCheck
from data_check.checks.pipeline_check import PipelineCheck
from data_check.result import DataCheckResult


@pytest.fixture
def pc(dc_serial: DataCheck) -> PipelineCheck:
    _pc = PipelineCheck(dc_serial, Path())
    return _pc


def check_table_exists(table_name: str, dc_serial: DataCheck) -> bool:
    try:
        dc_serial.sql.run_query(f"select * from {table_name}")
        return True
    except Exception:
        return False


def test_is_pipeline_check():
    assert PipelineCheck.is_check_path(Path("checks/pipelines/simple_pipeline"))


def test_simple_pipeline(pc: PipelineCheck):
    pc.check_path = Path("checks/pipelines/simple_pipeline")
    result = pc.run_test()
    assert result


def test_pipeline_sqls_are_not_in_simple_tests(dc_serial: DataCheck):
    checks = dc_serial.collect_checks([Path("checks")])
    pipeline_test_sql = Path(
        "checks/pipelines/simple_pipeline/pipeline_checks/test_simple_pipeline.sql"
    )
    assert pipeline_test_sql.exists()
    assert pipeline_test_sql not in checks


def test_run_test_returns_data_check_result(pc: PipelineCheck):
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
    result.prepare_message(pc.data_check.output.prepare_data_check_result)
    assert "with exception" in result.message


def test_pipeline_fails_load_tables_no_csv_file(pc: PipelineCheck):
    pc.check_path = Path("checks/pipelines/failing/load_tables_no_csv_file")
    result = pc.run_test()
    assert not result
    result.prepare_message(pc.data_check.output.prepare_data_check_result)
    assert "with exception" in result.message


def test_pipeline_fails_load_tables_invalid_load_mode(pc: PipelineCheck):
    pc.check_path = Path("checks/pipelines/failing/load_tables_invalid_load_mode")
    result = pc.run_test()
    assert not result
    result.prepare_message(pc.data_check.output.prepare_data_check_result)
    assert "with exception" in result.message
    assert "unknown load mode: invalid" in str(result.exception)


def test_pipeline_fails_invalid_cmd(pc: PipelineCheck):
    pc.check_path = Path("checks/pipelines/failing/invalid_cmd")
    result = pc.run_test()
    assert not result


def test_pipeline_fails_run_sql_no_file(pc: PipelineCheck):
    pc.check_path = Path("checks/pipelines/failing/run_sql_no_file")
    result = pc.run_test()
    assert not result
    result.prepare_message(pc.data_check.output.prepare_data_check_result)
    assert "with exception" in result.message


def test_pipeline_fails_run_sql_invalid_query(pc: PipelineCheck):
    pc.check_path = Path("checks/pipelines/failing/run_sql_invalid_query")
    result = pc.run_test()
    assert not result
    result.prepare_message(pc.data_check.output.prepare_data_check_result)
    assert "with exception" in result.message


def test_pipeline_fails_always_run_failing(pc: PipelineCheck):
    pc.check_path = Path("checks/pipelines/failing/always_run")
    result = pc.run_test()
    assert not result
    result.prepare_message(pc.data_check.output.prepare_data_check_result)
    assert "with exception" in result.message
    assert not check_table_exists("always_run_1", pc.data_check)
    assert check_table_exists("always_run_2", pc.data_check)


def test_pipeline_always_run_in_between(pc: PipelineCheck):
    pc.check_path = Path("checks/pipelines/always_run_in_between")
    result = pc.run_test()
    assert result
    assert check_table_exists("always_run_b1", pc.data_check)
    assert check_table_exists("always_run_b2", pc.data_check)
    assert check_table_exists("always_run_b3", pc.data_check)


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
    print(result.exception)
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
    assert project_path == Path().absolute()


def test_pipeline_template_connection(pc: PipelineCheck):
    tp = pc.template_parameters(pipeline_path=Path("checks/pipelines/simple_pipeline"))
    connection = tp["CONNECTION"]
    assert connection == "test"


def test_pipeline_template_connection_string(pc: PipelineCheck):
    tp = pc.template_parameters(pipeline_path=Path("checks/pipelines/simple_pipeline"))
    connection_string = tp["CONNECTION_STRING"]
    assert connection_string == "sqlite+pysqlite://"


def test_pipeline_stops_after_failing_step(pc: PipelineCheck):
    pc.check_path = Path("checks/pipelines/failing/pipeline_stops")
    result = pc.run_test()
    assert not result
    assert check_table_exists("pipeline_stop_b1", pc.data_check)
    assert not check_table_exists("pipeline_stop_b2", pc.data_check)


def test_sql_files_is_deprecated(pc: PipelineCheck):
    steps = [{"sql_files": ["run_sql/run_test.sql"]}]
    pc._pipeline_config = {"steps": steps}
    with pytest.warns(FutureWarning, match="sql_files is deprecated, use"):
        pc.run_test()


def test_sql_file_alias(pc: PipelineCheck):
    steps = [{"sql": {"file": "run_sql/run_test.sql"}}]
    pc._pipeline_config = {"steps": steps}
    result = pc.run_test()
    assert result


def test_sql_files_and_file_simultaneously_raise_exception(pc: PipelineCheck):
    steps = [{"sql": {"file": "run_sql/run_test.sql", "files": "run_sql/run_test.sql"}}]
    pc._pipeline_config = {"steps": steps}
    result = pc.run_test()
    assert result.exception


def test_run_is_alias_for_check(pc: PipelineCheck):
    steps = [{"run": ["checks/basic"]}]
    pc._pipeline_config = {"steps": steps}
    result = pc.run_test()
    assert result


def test_write_check_in_pipeline(pc: PipelineCheck, tmp_path: Path):
    pc.check_path = tmp_path
    steps = [{"sql": {"query": "select 1 as a", "write_check": "a.sql"}}]
    pc._pipeline_config = {"steps": steps}
    pc.run_test()
    assert (tmp_path / "a.sql").exists()
    assert (tmp_path / "a.csv").exists()


def test_output_with_files(pc: PipelineCheck):
    steps = [{"sql": {"files": "run_sql/run_test.sql", "output": "run_test.csv"}}]
    pc._pipeline_config = {"steps": steps}
    result = pc.run_test()
    assert result
    assert not Path("run_test.csv").exists()


def test_load_table_is_deprecated(pc: PipelineCheck):
    steps = [
        {
            "load_table": {
                "table": "test_load_table_is_deprecated",
                "file": "load_data/test.csv",
            }
        }
    ]
    pc._pipeline_config = {"steps": steps}
    with pytest.warns(FutureWarning, match="load_table is deprecated, use"):
        pc.run_test()


def test_load_files_and_file_simultaneously_raise_exception(pc: PipelineCheck):
    steps = [{"load": {"file": "load_data/test.csv", "files": "load_data/test.csv"}}]
    pc._pipeline_config = {"steps": steps}
    result = pc.run_test()
    assert result.exception


def test_load_table_without_file_raise_exception(pc: PipelineCheck):
    steps = [{"load": {"table": "test_load_table_without_file_raise_exception"}}]
    pc._pipeline_config = {"steps": steps}
    result = pc.run_test()
    assert result.exception


def test_load_table_with_files_raise_exception(pc: PipelineCheck):
    steps = [
        {
            "load": {
                "table": "test_load_table_without_file_raise_exception",
                "files": "load_data/test.csv",
            }
        }
    ]
    pc._pipeline_config = {"steps": steps}
    result = pc.run_test()
    assert result.exception


def test_load_file_is_alias_for_files(pc: PipelineCheck):
    steps = [{"load": {"file": "load_data/test.csv"}}]
    pc._pipeline_config = {"steps": steps}
    result = pc.run_test()
    assert result


def test_pipeline_yaml_is_check_path():
    assert PipelineCheck.is_check_path(
        Path("checks/pipelines/simple_pipeline/data_check_pipeline.yml")
    )


def test_check_path_is_folder_when_using_pipeline_yaml(dc_serial: DataCheck):
    pc = PipelineCheck(
        dc_serial, Path("checks/pipelines/simple_pipeline/data_check_pipeline.yml")
    )
    assert pc.check_path == Path("checks/pipelines/simple_pipeline")

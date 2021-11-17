import pytest
from pathlib import Path


from data_check import DataCheck
from data_check.checks import (
    CSVCheck,
    PipelineCheck,
    EmptySetCheck,
    DataCheckGenerator,
    ExcelCheck,
)  # noqa E402
from data_check.config import DataCheckConfig

# Basic data_check unit tests


@pytest.fixture
def dc() -> DataCheck:
    config = DataCheckConfig().load_config().set_connection("test")
    _dc = DataCheck(config)
    _dc.load_template()
    _dc.output.configure_output(
        verbose=True,
        traceback=True,
        print_failed=True,
        print_format="json",
    )
    return _dc


@pytest.fixture
def dc_wo_template() -> DataCheck:
    config = DataCheckConfig().load_config().set_connection("test")
    _dc = DataCheck(config)
    _dc.output.configure_output(
        verbose=True,
        traceback=True,
        print_failed=True,
        print_format="json",
    )
    return _dc


def test_raise_exception_if_running_without_connection():
    config = DataCheckConfig()
    config.connection = str(None)
    dc = DataCheck(config)
    result = dc.get_check(Path("checks/basic/simple_string.sql")).run_test()
    assert not result
    assert "generated an exception" in result.result


def test_collect_checks(dc: DataCheck):
    # This test is also to ensure, that all checks are copied over to int_test
    checks = dc.collect_checks([Path("checks")])
    assert len(checks) == 39


def test_collect_checks_returns_sorted_list(dc: DataCheck):
    checks = dc.collect_checks(
        [
            Path("checks/templates"),
            Path("checks/basic"),
            Path("checks/generated"),
            Path("checks/pipelines"),
            Path("checks/failing"),
        ]
    )
    assert checks == sorted(checks)


def test_get_check_none(dc: DataCheck):
    check = dc.get_check(Path("checks/basic"))
    assert check is None


def test_get_check_csv_check(dc: DataCheck):
    check = dc.get_check(Path("checks/basic/simple_string.sql"))
    assert isinstance(check, CSVCheck)


def test_get_check_pipeline_check(dc: DataCheck):
    check = dc.get_check(Path("checks/pipelines/simple_pipeline"))
    assert isinstance(check, PipelineCheck)


def test_get_check_generator(dc: DataCheck):
    dc.config.generate_mode = True
    check = dc.get_check(Path("checks/basic/simple_string.sql"))
    assert isinstance(check, DataCheckGenerator)


def test_get_check_empty_set_check(dc: DataCheck):
    check = dc.get_check(Path("checks/empty_sets/basic/empty_query.sql"))
    assert isinstance(check, EmptySetCheck)


def test_get_check_excel_check(dc: DataCheck):
    check = dc.get_check(Path("checks/excel/basic/simple_excel.sql"))
    assert isinstance(check, ExcelCheck)


def test_run_fail_if_path_doesnt_exist(dc: DataCheck):
    result = dc.run([Path("non_existing_checks")])
    assert not result


def test_no_template_when_not_loaded(dc_wo_template: DataCheck):
    assert dc_wo_template.template_data == {}


def test_load_template(dc_wo_template: DataCheck):
    dc_wo_template.load_template()
    assert "date_type" in dc_wo_template.template_data


def test_load_template_without_template_folder(
    dc_wo_template: DataCheck, tmp_path: Path
):
    dc_wo_template.config.project_path = tmp_path
    dc_wo_template.load_template()
    assert dc_wo_template.template_data == {}


def test_no_lookups_when_not_loaded(dc: DataCheck):
    assert dc.lookup_data == {}


def test_load_lookups(dc: DataCheck):
    dc.load_lookups()
    assert dc.lookup_data == {"b1": ["b", "c"], "sub_lkp__b2": [1, 2]}


def test_load_lookups_without_lookup_folder(dc: DataCheck, tmp_path: Path):
    dc.config.project_path = tmp_path
    dc.load_lookups()
    assert dc.lookup_data == {}


def test_sql_params(dc: DataCheck):
    dc.load_lookups()
    assert dc.sql_params == dc.lookup_data

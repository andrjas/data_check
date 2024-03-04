from pathlib import Path
from unittest.mock import create_autospec

from data_check import DataCheck
from data_check.checks import (
    CSVCheck,
    DataCheckGenerator,  # noqa E402
    EmptySetCheck,
    ExcelCheck,
    PipelineCheck,
    TableCheck,
)
from data_check.config import DataCheckConfig
from data_check.sql.sql import DataCheckSql

# Basic data_check unit tests


def test_raise_exception_if_running_without_connection():
    config = DataCheckConfig()
    config.connection = str(None)
    dc = DataCheck(config)
    check = dc.get_check(Path("checks/basic/simple_string.sql"))
    assert check
    result = check.run_test()
    assert not result
    result.prepare_message(dc.output.prepare_data_check_result)
    assert "with exception in " in result.message


def test_collect_checks(dc: DataCheck):
    checks = dc.collect_checks([Path("checks")])
    min_checks_count = 41
    assert len(checks) >= min_checks_count


def test_collect_checks_sql_doesnt_return_table_check(dc: DataCheck):
    checks = dc.collect_checks([Path("checks")])
    paths = [c.check_path for c in checks]
    assert Path("checks/basic/simple_string.csv").absolute() not in paths


def test_collect_checks_files_unique(dc: DataCheck):
    checks = dc.collect_checks([Path("checks")])
    paths = [c.check_path for c in checks]
    paths_set = set(paths)
    assert len(paths) == len(paths_set)


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


def test_get_check_table_check(dc: DataCheck):
    check = dc.get_check(Path("checks/table_check/basic/sqlite_master.csv"))
    assert isinstance(check, TableCheck)


def test_get_check_csv_file_with_sql(dc: DataCheck):
    check = dc.get_check(Path("checks/basic/simple_string.csv"))
    assert isinstance(check, CSVCheck)


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


def test_run_sql_query_is_templated(dc: DataCheck):
    p = Path("checks/basic/simple_string.sql")
    assert "{{" in p.read_text()
    sql = create_autospec(DataCheckSql, instance=True)
    dc.sql = sql
    dc.run_sql_file(p)
    sql.run_sql.assert_called_once()
    _, kwargs = sql.run_sql.call_args_list[0]
    query = kwargs["query"]
    assert "{{" not in query

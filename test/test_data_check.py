import sys
import os
import pytest
from pathlib import Path


# add the parent path to PYTHONPATH
my_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, my_path + "/../")

from data_check import DataCheck  # noqa E402
from data_check.config import DataCheckConfig  # noqa E402

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


def test_run_test(dc: DataCheck):
    result = dc.run_test(Path("checks/basic/simple_string.sql"))
    assert result


def test_run_test_faling(dc: DataCheck):
    result = dc.run_test(Path("checks/failing/expected_to_fail.sql"))
    assert not result


def test_run_test_non_existent(dc: DataCheck):
    result = dc.run_test(Path("checks/non_existent/file.sql"))
    assert not result
    assert "NO EXPECTED RESULTS FILE" in result.result


def test_run_test_file(dc: DataCheck):
    result = dc.run([Path("checks/basic/simple_string.sql")])
    assert result


def test_run_test_folder(dc: DataCheck):
    result = dc.run([Path("checks/basic")])
    assert result


def test_raise_exception_if_running_without_connection():
    config = DataCheckConfig()
    config.connection = str(None)
    dc = DataCheck(config)
    result = dc.run_test(Path("checks/basic/simple_string.sql"))
    assert not result
    assert "generated an exception" in result.result


def test_run_files_failing(dc: DataCheck):
    result = dc.run([Path("checks/failing")])
    assert not result


def test_run_invalid(dc: DataCheck):
    result = dc.run([Path("checks/failing/invalid.sql")])
    assert not result


def test_template(dc: DataCheck):
    result = dc.run([Path("checks/templates/template1.sql")])
    assert result


def test_run_test_invalid_csv(dc: DataCheck):
    result = dc.run_test(Path("checks/failing/invalid_csv.sql"))
    assert not result


def test_run_sql_file(dc: DataCheck):
    result = dc.run_sql_file(Path("run_sql/run_test.sql"))
    assert result


def test_run_sql_files(dc: DataCheck):
    results = dc.run_sql_files([Path("run_sql")])
    assert results


def test_collect_checks(dc: DataCheck):
    # This test is also to ensure, that all checks are copied over to int_test
    checks = dc.collect_checks([Path("checks")])
    assert len(checks) == 26


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


def test_run_test_faling_duplicates(dc: DataCheck):
    result = dc.run_test(Path("checks/failing/duplicates.sql"))
    assert not result

import sys
import os
import pytest
from pathlib import Path


# add the parent path to PYTHONPATH
my_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, my_path + "/../")

from data_check import DataCheck, DataCheckException  # noqa E402


@pytest.fixture
def dc() -> DataCheck:
    config = DataCheck.read_config()
    connection = config.get("connections", {}).get("test")
    return DataCheck(connection)


def test_read_config():
    config = DataCheck.read_config()
    assert config
    assert "connections" in config
    connections = config.get("connections", {})
    assert "test" in connections
    assert "test2" in connections


def test_get_expect_file(dc):
    ef = dc.get_expect_file(Path("test_file.sql"))
    assert ef == Path("test_file.csv")


def test_run_query(dc):
    result = dc.run_query("select 'ok' as test_col")
    assert result.iloc[0].test_col == "ok"


def test_run_test(dc):
    result = dc.run_test(Path("checks/basic/simple_string.sql"))
    assert result


def test_run_test_faling(dc):
    result = dc.run_test(Path("checks/failing/expected_to_fail.sql"))
    assert not result


def test_run_test_non_existent(dc):
    result = dc.run_test(Path("checks/non_existent/file.sql"))
    assert not result
    assert "NO EXPECTED RESULTS FILE" in result.result


def test_run_test_file(dc):
    result = dc.run([Path("checks/basic/simple_string.sql")])
    assert result


def test_run_test_folder(dc):
    result = dc.run([Path("checks/basic")])
    assert result


def test_raise_exception_if_running_without_connection():
    dc = DataCheck(connection=None)
    result = dc.run_test(Path("checks/basic/simple_string.sql"))
    assert not result
    assert "generated an exception" in result.result


def test_expand_files(dc):
    files = dc.expand_files([Path("checks/basic"), Path("checks/failing")])
    assert len(files) >= 3


def test_run_files_failing(dc):
    result = dc.run([Path("checks/failing")])
    assert not result


def test_run_invalid(dc):
    result = dc.run([Path("checks/failing/invalid.sql")])
    assert not result

import sys
import os
import pytest
from pathlib import Path


# add the parent path to PYTHONPATH
my_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, my_path + "/../")

from data_check import DataCheck  # noqa E402


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

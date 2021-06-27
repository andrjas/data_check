import sys
import os
import pytest
from pathlib import Path


# add the parent path to PYTHONPATH
my_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, my_path + "/../")

from data_check.data_check_sql import DataCheckSql  # noqa E402
from data_check import DataCheck  # noqa E402


@pytest.fixture
def sql() -> DataCheckSql:
    config = DataCheck.read_config()
    connection = config.get("connections", {}).get("test")
    _sql = DataCheckSql(connection)
    return _sql


def test_run_query(sql):
    if "oracle" in sql.connection:
        result = sql.run_query("select 'ok' as test_col from dual")
    else:
        result = sql.run_query("select 'ok' as test_col")
    assert result.iloc[0].test_col == "ok"


def test_test_connection(sql):
    test = sql.test_connection()
    assert test

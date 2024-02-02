from pathlib import Path

import pytest

from data_check import DataCheck
from data_check.config import DataCheckConfig
from data_check.sql import DataCheckSql, get_sql


@pytest.fixture(autouse=True)
def change_test_dir(monkeypatch: pytest.MonkeyPatch):
    """Switch to example folder when running test.
    If we are in int_test, the folder might not exists,
    since we are in the right project folder already.
    """
    if Path("example").exists():
        monkeypatch.chdir("example")


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
def dc_serial() -> DataCheck:
    config = DataCheckConfig().load_config().set_connection("test")
    config.parallel_workers = (
        1  # since we do not persist anything in SQLite, we must use a single connection
    )
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


@pytest.fixture
def sql() -> DataCheckSql:
    dc_config = DataCheckConfig().load_config().set_connection("test")
    _sql = get_sql(dc_config.connection)
    return _sql

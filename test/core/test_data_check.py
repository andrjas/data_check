import pytest
from pathlib import Path


from data_check import DataCheck  # noqa E402
from data_check.config import DataCheckConfig
from data_check.checks.csv_check import CSVCheck  # noqa E402

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
    assert len(checks) == 28


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


def test_get_check(dc: DataCheck):
    check = dc.get_check(Path("checks/basic/simple_string.sql"))
    assert isinstance(check, CSVCheck)

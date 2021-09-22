import pytest
from pathlib import Path


from data_check import DataCheck
from data_check.config import DataCheckConfig


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


def test_empty_set_check(dc: DataCheck):
    check = dc.get_check(Path("checks/empty_sets/basic/empty_query.sql"))
    result = check.run_test()
    assert result


def test_empty_set_check_failing(dc: DataCheck):
    check = dc.get_check(Path("checks/empty_sets/failing/not_empty_query.sql"))
    result = check.run_test()
    assert not result

from pathlib import Path


from data_check import DataCheck


def test_empty_set_check(dc: DataCheck):
    check = dc.get_check(Path("checks/empty_sets/basic/empty_query.sql"))
    result = check.run_test()
    assert result


def test_empty_set_check_failing(dc: DataCheck):
    check = dc.get_check(Path("checks/empty_sets/failing/not_empty_query.sql"))
    result = check.run_test()
    assert not result

from pathlib import Path

from data_check import DataCheck


def test_simple_excel_check(dc: DataCheck):
    check = dc.get_check(Path("checks/excel/basic/simple_excel.sql"))
    result = check.run_test()
    assert result


def test_empty_query(dc: DataCheck):
    check = dc.get_check(Path("checks/excel/basic/empty.sql"))
    result = check.run_test()
    assert result


def test_data_types(dc: DataCheck):
    check = dc.get_check(Path("checks/excel/basic/data_types.sql"))
    result = check.run_test()
    assert result


def test_failing(dc: DataCheck):
    check = dc.get_check(Path("checks/excel/failing/failing_excel.sql"))
    result = check.run_test()
    assert not result


def test_failing_empty(dc: DataCheck):
    check = dc.get_check(Path("checks/excel/failing/failing_empty.sql"))
    result = check.run_test()
    assert not result

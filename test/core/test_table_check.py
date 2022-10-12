from pathlib import Path


from data_check import DataCheck

# Basic data_check unit tests


def test_table_check(dc: DataCheck):
    check = dc.get_check(Path("checks/table_check/basic/sqlite_master.csv"))
    assert check
    result = check.run_test()
    assert result


def test_table_check_wrong_column(dc: DataCheck):
    check = dc.get_check(
        Path("checks/table_check/failing/wrong_column/sqlite_master.csv")
    )
    assert check
    result = check.run_test()
    assert not result


def test_table_check_wrong_data(dc: DataCheck):
    check = dc.get_check(
        Path("checks/table_check/failing/wrong_data/sqlite_master.csv")
    )
    assert check
    result = check.run_test()
    assert not result


def test_table_check_pipeline(dc_serial: DataCheck):
    check = dc_serial.get_check(Path("checks/pipelines/table_check"))
    assert check
    result = check.run_test()
    print(result)
    assert result

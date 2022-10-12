from pathlib import Path


from data_check import DataCheck  # noqa E402

# Basic data_check unit tests


def test_run_test(dc: DataCheck):
    check = dc.get_check(Path("checks/basic/simple_string.sql"))
    assert check
    result = check.run_test()
    assert result


def test_run_test_faling(dc: DataCheck):
    check = dc.get_check(Path("checks/failing/expected_to_fail.sql"))
    assert check
    result = check.run_test()
    assert not result


def test_run_test_file(dc: DataCheck):
    result = dc.run([Path("checks/basic/simple_string.sql")])
    assert result


def test_run_test_folder(dc: DataCheck):
    result = dc.run([Path("checks/basic")])
    assert result


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
    check = dc.get_check(Path("checks/failing/invalid_csv.sql"))
    assert check
    result = check.run_test()
    assert not result


def test_run_sql_file(dc: DataCheck):
    result = dc.run_sql_file(Path("run_sql/run_test.sql"))
    assert result


def test_run_sql_files(dc: DataCheck):
    results = dc.run_sql_files([Path("run_sql")])
    assert results


def test_run_test_faling_duplicates(dc: DataCheck):
    check = dc.get_check(Path("checks/failing/duplicates.sql"))
    assert check
    result = check.run_test()
    assert not result

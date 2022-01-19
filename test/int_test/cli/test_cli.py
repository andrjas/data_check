from click.testing import CliRunner, Result
import os
from pathlib import Path
import shutil
from os import linesep, sep
from typing import List, Optional

from data_check.cli.main import main


def run(command: List[str], workers: Optional[int] = 1) -> Result:
    runner = CliRunner()
    if not workers:
        workers_cmd = []
    else:
        workers_cmd = ["-n", str(workers)]
    result = runner.invoke(main, command + workers_cmd)
    return result


def run_check(command: List[str], workers: Optional[int] = 1):
    return run(command + ["checks/basic/simple_string.sql"], workers=workers)


def assert_passed(result: Result):
    assert result.exit_code == 0
    assert (
        result.output.replace("\x1b[0m", "").replace("\x1b[32m", "")
        == f"""checks{sep}basic{sep}simple_string.sql: PASSED

overall result: PASSED
"""
    )


def assert_failed(result: Result):
    assert result.exit_code == 1
    assert "overall result: FAILED" in result.output


def test_help():
    res = run(["--help"])
    assert res.exit_code == 0
    assert res.output.startswith("Usage:")


def all_options():
    res = run(["--help"])
    lines = res.output.split("Options:", maxsplit=1)[1].strip().splitlines()
    options: List[str] = []
    for opt in lines:
        opt = opt.strip()
        if opt and opt.startswith("-"):
            for o in opt.split(" "):
                if o.startswith("-"):
                    o = o.rstrip(",")
                    options.append(o)
    return options


def test_all_options_tested():
    dc_options = all_options()
    tested_options = Path(__file__).read_text()

    join: List[str] = []
    for option in dc_options:
        if f'"{option}"' in tested_options:
            join.append(option)

    assert set(join) == set(dc_options)


def test_single_file_check():
    res = run_check([])
    assert_passed(res)


def test_run_without_parameters():
    res = run([], workers=None)
    assert_failed(res)


def test_failing():
    res = run(["checks/failing/expected_to_fail.sql"])
    assert_failed(res)


def test_invalid():
    res = run(["checks/failing/invalid.sql"])
    assert_failed(res)
    assert (
        f"FAILED (with exception in checks{sep}failing{sep}invalid.sql)" in res.output
    )


def test_c():
    out = run_check(["-c", "test2"])
    assert_passed(out)


def test_connection():
    out = run_check(["--connection", "test2"])
    assert_passed(out)


def test_unknown_connection():
    res = run_check(["--connection", "unknown"])
    assert res.exit_code == 1
    assert "unknown connection: unknown" in res.output


def test_n():
    out = run_check(["-n", "1"])
    assert_passed(out)


def test_workers():
    out = run_check(["--workers", "1"])
    assert_passed(out)


def test_workers_invalid_number():
    res = run_check(["--workers", "a"], workers=None)
    assert res.exit_code == 2


def test_print():
    res = run(["--print", "checks/failing/expected_to_fail.sql"])
    assert_failed(res)
    assert "check1,_diff" in res.output


def test_print_json():
    res = run(["--print-json", "checks/failing/expected_to_fail.sql"])
    assert res.exit_code == 1


def test_print_format_csv():
    res = run(
        [
            "--print",
            "--print-format",
            "csv",
            "checks/failing/expected_to_fail.sql",
        ]
    )
    assert_failed(res)
    assert "check1,_diff" in res.output


def test_print_format_pandas():
    res = run(
        [
            "--print",
            "--print-format",
            "pandas",
            "checks/failing/expected_to_fail.sql",
        ]
    )
    assert_failed(res)
    assert "_diff" in res.output


def test_print_format_unknown():
    res = run(
        [
            "--print",
            "--print-format",
            "unknown",
            "checks/failing/expected_to_fail.sql",
        ]
    )
    assert res.exit_code == 1


def test_print_diff():
    res = run(["--print", "--diff", "checks/failing/diff.sql"])
    assert_failed(res)
    assert "id,x,_diff" in res.output


def test_config():
    out = run_check(["--config", "data_check.yml"])
    assert_passed(out)


def test_config_invalid_path():
    res = run_check(["--config", "dos_not_exists.yml"])
    assert res.exit_code == 1


def test_ping():
    res = run(["--ping"])
    assert res.exit_code == 0
    assert res.output.strip() == "connecting succeeded"


def test_invalid_verbose():
    res = run(["--verbose", "checks/failing/invalid.sql"])
    assert_failed(res)
    assert (
        f"FAILED (with exception in checks{sep}failing{sep}invalid.sql)" in res.output
    )
    assert '(sqlite3.OperationalError) near "selct": syntax error' in res.output


def test_invalid_traceback():
    res = run(["--traceback", "checks/failing/invalid.sql"])
    assert_failed(res)
    assert (
        f"FAILED (with exception in checks{sep}failing{sep}invalid.sql)" in res.output
    )
    assert '(sqlite3.OperationalError) near "selct": syntax error' in res.output
    assert "Traceback (most recent call last):" in res.output


def test_version():
    res = run(["--version"])
    assert res.exit_code == 0
    assert ", version" in res.output


def test_g():
    gen_csv = Path("checks/generated/generate_before_running_g.csv")
    gen_sql_org = Path("checks/generated/generate_before_running.sql")
    gen_sql = Path("checks/generated/generate_before_running_g.sql")
    if gen_csv.exists():
        os.unlink(gen_csv)
    if gen_sql.exists():
        os.unlink(gen_sql)
    shutil.copy(gen_sql_org, gen_sql)
    res = run(["-g", str(gen_sql)])
    assert res.exit_code == 0
    assert gen_csv.exists()
    os.unlink(gen_csv)
    os.unlink(gen_sql)
    assert res.output.startswith(
        f"expectation written to checks{sep}generated{sep}generate_before_running_g.csv"
    )


def test_generate():
    gen_csv = Path("checks/generated/generate_before_running.csv")
    if gen_csv.exists():
        os.unlink(gen_csv)
    res = run(["--generate", "checks/generated/generate_before_running.sql"])
    assert res.exit_code == 0
    assert gen_csv.exists()
    os.unlink(gen_csv)
    assert res.output.startswith(
        f"expectation written to checks{sep}generated{sep}generate_before_running.csv"
    )


def test_generate_no_overwrite_without_force():
    gen_csv = Path("checks/generated/generate_before_running.csv")
    if gen_csv.exists():
        os.unlink(gen_csv)
    gen_csv.write_text("")
    res = run(["--generate", "checks/generated/generate_before_running.sql"])
    assert res.exit_code == 0
    assert gen_csv.exists()
    assert gen_csv.read_text() == ""
    os.unlink(gen_csv)
    assert res.output.startswith(
        f"expectation skipped for checks{sep}generated{sep}generate_before_running.csv"
    )


def test_generate_force():
    gen_csv = Path("checks/generated/generate_before_running.csv")
    if gen_csv.exists():
        os.unlink(gen_csv)
    gen_csv.write_text("")
    res = run(
        [
            "--generate",
            "--force",
            "checks/generated/generate_before_running.sql",
        ]
    )
    assert res.exit_code == 0
    assert gen_csv.exists()
    assert gen_csv.read_text() != ""
    os.unlink(gen_csv)
    assert res.output.startswith(
        f"expectation written to checks{sep}generated{sep}generate_before_running.csv"
    )


def test_sql_file():
    res = run(["--sql-file", "run_sql/run_test.sql"])
    assert res.exit_code == 0
    assert "executing:" in res.output
    assert "select 1 as a" in res.output


def test_sql_files():
    res = run(["--sql-files", "run_sql"])
    assert res.exit_code == 0
    print(res.output)
    assert "executing:" in res.output
    assert "select 1 as a" in res.output


def test_sql_file_failing_sql():
    res = run(["--sql-file", "failing/run_sql/invalid.sql"])
    assert res.exit_code == 1


def test_sql_file_no_file():
    res = run(["--sql-file", "failing/run_sql/no_such_file.sql"])
    assert res.exit_code == 1


def test_load_tables():
    res = run(["--load-tables", "load_data/test.csv"])
    assert res.exit_code == 0
    assert res.output.strip() == f"table test loaded from load_data{sep}test.csv"


def test_load_tables_folder(caplog):
    res = run(["--load-tables", "load_data/tables"])
    assert res.exit_code == 0
    assert f"table test1 loaded from load_data{sep}tables{sep}test1.csv" in res.output
    assert f"table test3 loaded from load_data{sep}tables{sep}test3.xlsx" in res.output
    assert (
        f"table main.test2 loaded from load_data{sep}tables{sep}main.test2.csv"
        in res.output
    )


def test_sql():
    res = run(["--sql", "select 1 as a"])
    assert res.exit_code == 0
    assert res.output.strip() == f"a{linesep}1"


def test_sql_output():
    test_output = Path("test_sql_output_1.csv")
    if test_output.exists():
        test_output.unlink()
    res = run(["--sql", "select 1 as a", "--output", str(test_output)])
    assert res.exit_code == 0
    exists = test_output.exists()
    test_output.unlink()
    assert res.output.strip() == f"a{linesep}1"
    assert exists


def test_sql_output_o():
    test_output = Path("test_sql_output_2.csv")
    if test_output.exists():
        test_output.unlink()
    res = run(["--sql", "select 1 as a", "-o", str(test_output)])
    assert res.exit_code == 0
    exists = test_output.exists()
    test_output.unlink()
    assert res.output.strip() == f"a{linesep}1"
    assert exists


def test_sql_invalid_query():
    res = run(["--sql", "selct 1 as a"])
    assert res.exit_code == 1


def test_load_table():
    res = run(["--load", "load_data/test.csv", "--table", "test_load_table"])
    assert res.exit_code == 0
    assert (
        res.output.strip()
        == f"table test_load_table loaded from load_data{sep}test.csv"
    )


def test_load_table_excel():
    res = run(
        [
            "--load",
            "load_data/test.xlsx",
            "--table",
            "test_load_table_excel",
        ]
    )
    assert res.exit_code == 0
    assert (
        res.output.strip()
        == f"table test_load_table_excel loaded from load_data{sep}test.xlsx"
    )


def test_load_mode_truncate():
    res = run(
        [
            "--load",
            "load_data/test.csv",
            "--table",
            "test_load_table_truncate",
            "--load-mode",
            "truncate",
        ]
    )
    assert res.exit_code == 0
    assert (
        res.output.strip()
        == f"table test_load_table_truncate loaded from load_data{sep}test.csv"
    )


def test_load_mode_append():
    res = run(
        [
            "--load",
            "load_data/test.csv",
            "--table",
            "test_load_table_append",
            "--load-mode",
            "append",
        ]
    )
    assert res.exit_code == 0
    assert (
        res.output.strip()
        == f"table test_load_table_append loaded from load_data{sep}test.csv"
    )


def test_load_mode_replace():
    res = run(
        [
            "--load",
            "load_data/test.csv",
            "--table",
            "test_load_table_replace",
            "--load-mode",
            "replace",
        ]
    )
    assert res.exit_code == 0
    assert (
        res.output.strip()
        == f"table test_load_table_replace loaded from load_data{sep}test.csv"
    )


def test_load_mode_invalid():
    res = run(
        [
            "--load",
            "load_data/test.csv",
            "--table",
            "test_load_table_invalid",
            "--load-mode",
            "invalid",
        ]
    )
    assert res.exit_code == 1


def test_load_mode_with_load_tables():
    res = run(["--load-tables", "load_data/test.csv", "--load-mode", "append"])
    assert res.exit_code == 0
    assert res.output.strip() == f"table test loaded from load_data{sep}test.csv"


def test_quiet():
    res = run(["--ping", "--quiet"])
    assert res.exit_code == 0
    assert res.output.strip() == ""


def test_log():
    log = Path("test_log.txt")
    if log.exists():
        log.unlink()
    res = run(["checks/basic/simple_string.sql", "--log", str(log)])
    assert res.exit_code == 0
    log_text = log.read_text()
    log_exists = log.exists()
    log.unlink()
    assert log_exists
    assert log_text.strip() != ""


def test_log_quiet():
    log = Path("test_log_quiet.txt")
    if log.exists():
        log.unlink()
    res = run(["checks/basic/simple_string.sql", "--log", str(log), "--quiet"])
    assert res.exit_code == 0
    log_text = log.read_text()
    log_exists = log.exists()
    log.unlink()
    assert log_exists
    assert log_text.strip() != ""


def test_write_check(tmp_path: Path):
    check_sql = tmp_path / "write_check.sql"
    check_csv = tmp_path / "write_check.csv"
    res = run(["--sql", "select 1 as a", "--write-check", str(check_sql)])
    assert res.exit_code == 0
    assert check_sql.exists()
    assert check_csv.exists()
    assert check_sql.read_text() != ""
    assert check_csv.read_text() != ""


def test_write_check_W(tmp_path: Path):
    check_sql = tmp_path / "write_check.sql"
    check_csv = tmp_path / "write_check.csv"
    res = run(["--sql", "select 1 as a", "-W", str(check_sql)])
    assert res.exit_code == 0
    assert check_sql.exists()
    assert check_csv.exists()
    assert check_sql.read_text() != ""
    assert check_csv.read_text() != ""

import pytest
from subprocess import check_output, CalledProcessError
import os
from pathlib import Path
import shutil
from os import sep
from typing import List


def run(command: List[str]):
    return check_output(command, universal_newlines=True)


def run_check(command: List[str]):
    return run(command + ["checks/basic/simple_string.sql"])


def assert_passed(out: str):
    assert (
        out.replace("\x1b[0m", "").replace("\x1b[32m", "")
        == f"""checks{sep}basic{sep}simple_string.sql: PASSED

overall result: PASSED
"""
    )


def assert_failed(out: str):
    assert "overall result: FAILED" in out


def test_help():
    out = run(["data_check", "--help"])
    assert out.startswith("Usage: data_check")


def all_options():
    help_output = run(["data_check", "--help"])
    lines = help_output.split("Options:", maxsplit=1)[1].strip().splitlines()
    options = []
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

    join = []
    for option in dc_options:
        if f'"{option}"' in tested_options:
            join.append(option)

    assert set(join) == set(dc_options)


def test_single_file_check():
    out = run_check(["data_check"])
    assert_passed(out)


def test_run_without_parameters():
    with pytest.raises(CalledProcessError) as e:
        run(["data_check"])
    assert_failed(e.value.output)


def test_failing():
    with pytest.raises(CalledProcessError) as e:
        run(["data_check", "checks/failing/expected_to_fail.sql"])
    assert_failed(e.value.output)


def test_invalid():
    with pytest.raises(CalledProcessError) as e:
        run(["data_check", "checks/failing/invalid.sql"])
    assert_failed(e.value.output)
    assert (
        f"FAILED (with exception in checks{sep}failing{sep}invalid.sql)"
        in e.value.output
    )


def test_c():
    out = run_check(["data_check", "-c", "test2"])
    assert_passed(out)


def test_connection():
    out = run_check(["data_check", "--connection", "test2"])
    assert_passed(out)


def test_unknown_connection():
    with pytest.raises(CalledProcessError) as e:
        run_check(["data_check", "--connection", "unknown"])

    assert "unknown connection: unknown" in e.value.output


def test_n():
    out = run_check(["data_check", "-n", "1"])
    assert_passed(out)


def test_workers():
    out = run_check(["data_check", "--workers", "1"])
    assert_passed(out)


def test_workers_invalid_number():
    with pytest.raises(CalledProcessError):
        run_check(["data_check", "--workers", "a"])


def test_print():
    with pytest.raises(CalledProcessError) as e:
        run(["data_check", "--print", "checks/failing/expected_to_fail.sql"])
    assert_failed(e.value.output)
    assert "check1,_diff" in e.value.output


def test_print_json():
    with pytest.raises(CalledProcessError) as e:
        run(["data_check", "--print-json", "checks/failing/expected_to_fail.sql"])


def test_print_format_csv():
    with pytest.raises(CalledProcessError) as e:
        run(
            [
                "data_check",
                "--print",
                "--print-format",
                "csv",
                "checks/failing/expected_to_fail.sql",
            ]
        )
    assert_failed(e.value.output)
    assert "check1,_diff" in e.value.output


def test_print_format_pandas():
    with pytest.raises(CalledProcessError) as e:
        run(
            [
                "data_check",
                "--print",
                "--print-format",
                "pandas",
                "checks/failing/expected_to_fail.sql",
            ]
        )
    assert_failed(e.value.output)
    assert "_diff" in e.value.output


def test_print_format_unknown():
    with pytest.raises(CalledProcessError):
        run(
            [
                "data_check",
                "--print",
                "--print-format",
                "unknown",
                "checks/failing/expected_to_fail.sql",
            ]
        )


def test_config():
    out = run_check(["data_check", "--config", "data_check.yml"])
    assert_passed(out)


def test_config_invalid_path():
    with pytest.raises(CalledProcessError):
        run_check(["data_check", "--config", "dos_not_exists.yml"])


def test_ping():
    out = run(["data_check", "--ping"])
    assert out.strip() == "connecting succeeded"


def test_invalid_verbose():
    with pytest.raises(CalledProcessError) as e:
        run(["data_check", "--verbose", "checks/failing/invalid.sql"])
    assert_failed(e.value.output)
    assert (
        f"FAILED (with exception in checks{sep}failing{sep}invalid.sql)"
        in e.value.output
    )
    assert '(sqlite3.OperationalError) near "selct": syntax error' in e.value.output


def test_invalid_traceback():
    with pytest.raises(CalledProcessError) as e:
        run(["data_check", "--traceback", "checks/failing/invalid.sql"])
    assert_failed(e.value.output)
    assert (
        f"FAILED (with exception in checks{sep}failing{sep}invalid.sql)"
        in e.value.output
    )
    assert '(sqlite3.OperationalError) near "selct": syntax error' in e.value.output
    assert "Traceback (most recent call last):" in e.value.output


def test_version():
    out = run(["data_check", "--version"])
    assert out.startswith("data_check, version")


def test_g():
    gen_csv = Path("checks/generated/generate_before_running_g.csv")
    gen_sql_org = Path("checks/generated/generate_before_running.sql")
    gen_sql = Path("checks/generated/generate_before_running_g.sql")
    if gen_csv.exists():
        os.unlink(gen_csv)
    if gen_sql.exists():
        os.unlink(gen_sql)
    shutil.copy(gen_sql_org, gen_sql)
    out = run(["data_check", "-g", str(gen_sql)])
    assert gen_csv.exists()
    os.unlink(gen_csv)
    os.unlink(gen_sql)
    assert out.startswith(
        f"expectation written to checks{sep}generated{sep}generate_before_running_g.csv"
    )


def test_gen():
    gen_csv = Path("checks/generated/generate_before_running_gen.csv")
    gen_sql_org = Path("checks/generated/generate_before_running.sql")
    gen_sql = Path("checks/generated/generate_before_running_gen.sql")
    if gen_csv.exists():
        os.unlink(gen_csv)
    if gen_sql.exists():
        os.unlink(gen_sql)
    shutil.copy(gen_sql_org, gen_sql)
    out = run(["data_check", "--gen", str(gen_sql)])
    assert gen_csv.exists()
    os.unlink(gen_csv)
    os.unlink(gen_sql)
    start_line = (
        f"expectation written to checks{sep}"
        f"generated{sep}generate_before_running_gen.csv"
    )
    assert out.startswith(start_line)


def test_generate():
    gen_csv = Path("checks/generated/generate_before_running.csv")
    if gen_csv.exists():
        os.unlink(gen_csv)
    out = run(
        ["data_check", "--generate", "checks/generated/generate_before_running.sql"]
    )
    assert gen_csv.exists()
    os.unlink(gen_csv)
    assert out.startswith(
        f"expectation written to checks{sep}generated{sep}generate_before_running.csv"
    )


def test_generate_no_overwrite_without_force():
    gen_csv = Path("checks/generated/generate_before_running.csv")
    if gen_csv.exists():
        os.unlink(gen_csv)
    gen_csv.write_text("")
    out = run(
        ["data_check", "--generate", "checks/generated/generate_before_running.sql"]
    )
    assert gen_csv.exists()
    assert gen_csv.read_text() == ""
    os.unlink(gen_csv)
    assert out.startswith(
        f"expectation skipped for checks{sep}generated{sep}generate_before_running.csv"
    )


def test_generate_force():
    gen_csv = Path("checks/generated/generate_before_running.csv")
    if gen_csv.exists():
        os.unlink(gen_csv)
    gen_csv.write_text("")
    out = run(
        [
            "data_check",
            "--generate",
            "--force",
            "checks/generated/generate_before_running.sql",
        ]
    )
    assert gen_csv.exists()
    assert gen_csv.read_text() != ""
    os.unlink(gen_csv)
    assert out.startswith(
        f"expectation written to checks{sep}generated{sep}generate_before_running.csv"
    )


def test_sql_file():
    out = run(["data_check", "--sql-file", "run_sql/run_test.sql"])
    print(out)
    assert "executing:" in out
    assert "select 1 as a" in out


def test_sql_files():
    out = run(["data_check", "--sql-files", "run_sql"])
    print(out)
    assert "executing:" in out
    assert "select 1 as a" in out


def test_sql_file_failing_sql():
    with pytest.raises(CalledProcessError):
        run(["data_check", "--sql-file", "failing/run_sql/invalid.sql"])


def test_sql_file_no_file():
    with pytest.raises(CalledProcessError):
        run(["data_check", "--sql-file", "failing/run_sql/no_such_file.sql"])


def test_load_tables():
    out = run(["data_check", "--load-tables", "load_data/test.csv"])
    assert out.strip() == "table test loaded from load_data/test.csv"


def test_load_tables_folder():
    out = run(["data_check", "--load-tables", "load_data"])
    assert "table test loaded from load_data/test.csv" in out
    assert "table main.test2 loaded from load_data/tables/main.test2.csv" in out


def test_sql():
    out = run(["data_check", "--sql", "select 1 as a"])
    assert out.strip() == "a\n1"


def test_sql_output():
    test_output = Path("test_sql_output_1.csv")
    if test_output.exists():
        test_output.unlink()
    out = run(["data_check", "--sql", "select 1 as a", "--output", str(test_output)])
    exists = test_output.exists()
    test_output.unlink()
    assert out.strip() == "a\n1"
    assert exists


def test_sql_output_o():
    test_output = Path("test_sql_output_2.csv")
    if test_output.exists():
        test_output.unlink()
    out = run(["data_check", "--sql", "select 1 as a", "-o", str(test_output)])
    exists = test_output.exists()
    test_output.unlink()
    assert out.strip() == "a\n1"
    assert exists


def test_sql_invalid_query():
    with pytest.raises(CalledProcessError) as e:
        run(["data_check", "--sql", "selct 1 as a"])


def test_load_table():
    out = run(
        ["data_check", "--load", "load_data/test.csv", "--table", "test_load_table"]
    )
    assert out.strip() == "table test_load_table loaded from load_data/test.csv"


def test_load_mode_truncate():
    out = run(
        [
            "data_check",
            "--load",
            "load_data/test.csv",
            "--table",
            "test_load_table_truncate",
            "--load-mode",
            "truncate",
        ]
    )
    assert (
        out.strip() == "table test_load_table_truncate loaded from load_data/test.csv"
    )


def test_load_mode_append():
    out = run(
        [
            "data_check",
            "--load",
            "load_data/test.csv",
            "--table",
            "test_load_table_append",
            "--load-mode",
            "append",
        ]
    )
    assert out.strip() == "table test_load_table_append loaded from load_data/test.csv"


def test_load_mode_replace():
    out = run(
        [
            "data_check",
            "--load",
            "load_data/test.csv",
            "--table",
            "test_load_table_replace",
            "--load-mode",
            "replace",
        ]
    )
    assert out.strip() == "table test_load_table_replace loaded from load_data/test.csv"


def test_load_mode_invalid():
    with pytest.raises(CalledProcessError) as e:
        run(
            [
                "data_check",
                "--load",
                "load_data/test.csv",
                "--table",
                "test_load_table_invalid",
                "--load-mode",
                "invalid",
            ]
        )


def test_load_mode_with_load_tables():
    out = run(
        ["data_check", "--load-tables", "load_data/test.csv", "--load-mode", "append"]
    )
    assert out.strip() == "table test loaded from load_data/test.csv"


def test_quiet():
    out = run(["data_check", "--ping", "--quiet"])
    assert out.strip() == ""

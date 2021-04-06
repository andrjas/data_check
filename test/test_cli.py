import pytest
from subprocess import check_output, CalledProcessError
import os
from pathlib import Path
import shutil
from os import sep


def run(command):
    return check_output(command, text=True)


def run_check(command):
    return run(command + ["checks/basic/simple_string.sql"])


def assert_passed(out):
    assert (
        out.replace("\x1b[0m", "").replace("\x1b[32m", "")
        == f"""checks{sep}basic{sep}simple_string.sql: PASSED

overall result: PASSED
"""
    )


def assert_failed(out):
    assert "overall result: FAILED" in out


def test_help():
    out = run(["data_check", "--help"])
    assert out.startswith("Usage: data_check")


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


@pytest.mark.skip
def test_unknown_connection():
    pass


def test_n():
    out = run_check(["data_check", "-n", "1"])
    assert_passed(out)


def test_workers():
    out = run_check(["data_check", "--workers", "1"])
    assert_passed(out)


def test_workers_invalid_number():
    with pytest.raises(CalledProcessError) as e:
        run_check(["data_check", "--workers", "a"])


def test_print():
    with pytest.raises(CalledProcessError) as e:
        run(["data_check", "--print", "checks/failing/expected_to_fail.sql"])
    assert_failed(e.value.output)
    assert "_diff" in e.value.output


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
    with pytest.raises(CalledProcessError) as e:
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
    with pytest.raises(CalledProcessError) as e:
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
    assert out.startswith(
        f"expectation written to checks{sep}generated{sep}generate_before_running_gen.csv"
    )


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

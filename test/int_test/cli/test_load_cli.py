from os import sep
from typing import Optional

from click.testing import CliRunner, Result

from data_check.cli.main import cli


def run(command: list[str], workers: Optional[int] = 1) -> Result:
    runner = CliRunner()
    workers_cmd = [] if not workers else ["-n", str(workers)]
    result = runner.invoke(cli, command + workers_cmd)
    return result


def test_load_tables():
    res = run(["load", "load_data/test.csv"])
    assert res.exit_code == 0
    assert res.output.strip() == f"table test loaded from load_data{sep}test.csv"


def test_load_tables_folder():
    res = run(["load", "load_data/tables"])
    assert res.exit_code == 0
    assert f"table test1 loaded from load_data{sep}tables{sep}test1.csv" in res.output
    assert f"table test3 loaded from load_data{sep}tables{sep}test3.xlsx" in res.output
    assert (
        f"table main.test2 loaded from load_data{sep}tables{sep}main.test2.csv"
        in res.output
    )


def test_load_table():
    res = run(["load", "load_data/test.csv", "--table", "test_load_table"])
    assert res.exit_code == 0
    assert (
        res.output.strip()
        == f"table test_load_table loaded from load_data{sep}test.csv"
    )


def test_load_table_excel():
    res = run(
        [
            "load",
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
            "load",
            "load_data/test.csv",
            "--table",
            "test_load_table_truncate",
            "--mode",
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
            "load",
            "load_data/test.csv",
            "--table",
            "test_load_table_append",
            "--mode",
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
            "load",
            "load_data/test.csv",
            "--table",
            "test_load_table_replace",
            "--mode",
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
            "load",
            "load_data/test.csv",
            "--table",
            "test_load_table_invalid",
            "--mode",
            "invalid",
        ]
    )
    assert res.exit_code == 1


def test_load_mode_with_load_tables():
    res = run(["load", "load_data/test.csv", "--mode", "append"])
    assert res.exit_code == 0
    assert res.output.strip() == f"table test loaded from load_data{sep}test.csv"


def test_append_tables():
    res = run(["append", "load_data/test.csv"])
    assert res.exit_code == 0
    assert res.output.strip() == f"table test loaded from load_data{sep}test.csv"


def test_append_tables_folder():
    res = run(["append", "load_data/tables"])
    assert res.exit_code == 0
    assert f"table test1 loaded from load_data{sep}tables{sep}test1.csv" in res.output
    assert f"table test3 loaded from load_data{sep}tables{sep}test3.xlsx" in res.output
    assert (
        f"table main.test2 loaded from load_data{sep}tables{sep}main.test2.csv"
        in res.output
    )


def test_append_table():
    res = run(["append", "load_data/test.csv", "--table", "test_load_table"])
    assert res.exit_code == 0
    assert (
        res.output.strip()
        == f"table test_load_table loaded from load_data{sep}test.csv"
    )


def test_append_table_excel():
    res = run(
        [
            "append",
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

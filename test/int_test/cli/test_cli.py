import shutil
import warnings
from os import linesep, sep
from pathlib import Path
from typing import List, Optional

from click.testing import CliRunner, Result

from data_check.cli.main import cli


def run(command: List[str], workers: Optional[int] = 1) -> Result:
    runner = CliRunner()
    if not workers:
        workers_cmd = []
    else:
        workers_cmd = ["-n", str(workers)]
    result = runner.invoke(cli, command + workers_cmd)
    return result


def write_dc_config(path: Path):
    Path(path / "data_check.yml").write_text(
        """
    default_connection: test
    connections:
        test: sqlite+pysqlite:///d.db
    """
    )


def run_check(command: List[str], workers: Optional[int] = 1):
    return run(command + ["checks/basic/simple_string.sql"], workers=workers)


def assert_passed(result: Result):
    assert result.exit_code == 0
    assert "summary: 1 passed, 0 failed, 0 warning" in result.output.replace(
        "\x1b[0m", ""
    ).replace("\x1b[32m", "")


def assert_failed(result: Result):
    assert result.exit_code == 1
    assert ": FAILED" in result.output


def test_help():
    res = run(["--help"])
    assert res.exit_code == 0
    assert res.output.startswith("Usage:")


def all_options():
    options: List[str] = []

    for subcommand in ("", "run", "gen", "load", "ping", "sql"):
        res = run([subcommand, "--help"])
        lines = res.output.split("Options:", maxsplit=1)[1].strip().splitlines()

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
    tested_options += ((Path(__file__).parent) / "test_load_cli.py").read_text()
    tested_options += ((Path(__file__).parent) / "test_init_cli.py").read_text()

    join: List[str] = [
        option for option in dc_options if f'"{option}"' in tested_options
    ]

    assert set(join) == set(dc_options)
    assert len(dc_options) > 10


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


def test_use_process():
    res = run_check(["--use-process"])
    assert_passed(res)


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
    res = run(["ping"])
    assert res.exit_code == 0
    assert res.output.strip() == "connecting succeeded"


def test_ping_wait():
    res = run(["ping", "--wait"])
    assert res.exit_code == 0
    assert res.output.strip() == "connecting succeeded"


def test_ping_wait_timeout():
    res = run(["ping", "--wait", "--timeout", "5"])
    assert res.exit_code == 0
    assert res.output.strip() == "connecting succeeded"


def test_ping_wait_timeout_retry():
    res = run(["ping", "--wait", "--timeout", "4", "--retry", "2"])
    assert res.exit_code == 0
    assert res.output.strip() == "connecting succeeded"


def test_ping_wait_retry():
    res = run(["ping", "--wait", "--retry", "2"])
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
        gen_csv.unlink()
    if gen_sql.exists():
        gen_sql.unlink()
    shutil.copy(gen_sql_org, gen_sql)
    res = run(["gen", str(gen_sql)])
    assert res.exit_code == 0
    assert gen_csv.exists()
    gen_csv.unlink()
    gen_sql.unlink()
    assert res.output.startswith(
        f"expectation written to checks{sep}generated{sep}generate_before_running_g.csv"
    )


def test_generate_no_overwrite_without_force():
    gen_csv = Path("checks/generated/generate_before_running.csv")
    if gen_csv.exists():
        gen_csv.unlink()
    gen_csv.write_text("")
    res = run(["gen", "checks/generated/generate_before_running.sql"])
    assert res.exit_code == 0
    assert gen_csv.exists()
    assert gen_csv.read_text() == ""
    gen_csv.unlink()
    assert res.output.startswith(
        f"expectation skipped for checks{sep}generated{sep}generate_before_running.csv"
    )


def test_generate_force():
    gen_csv = Path("checks/generated/generate_before_running.csv")
    if gen_csv.exists():
        gen_csv.unlink()
    gen_csv.write_text("")
    res = run(
        [
            "gen",
            "--force",
            "checks/generated/generate_before_running.sql",
        ]
    )
    assert res.exit_code == 0
    assert gen_csv.exists()
    assert gen_csv.read_text() != ""
    gen_csv.unlink()
    assert res.output.startswith(
        f"expectation written to checks{sep}generated{sep}generate_before_running.csv"
    )


def test_generate_full_table_check(tmp_path: Path):
    csv_txt = "type,name,tbl_name,rootpage,sql"
    runner = CliRunner()

    with runner.isolated_filesystem(tmp_path) as td:  # type: ignore
        tdp = Path(td)
        write_dc_config(tdp)
        runner.invoke(cli, ["sql", "create table a (i number)"])
        csv = "sqlite_master.csv"
        gen_csv = Path(csv)
        if gen_csv.exists():
            gen_csv.unlink()
        gen_csv.write_text(csv_txt)

        res = run(["gen", "--force", csv])

        assert res.exit_code == 0
        assert gen_csv.exists()
        assert not gen_csv.with_suffix(".sql").exists()

        assert gen_csv.read_text() != csv_txt


def test_sql_file():
    res = run(["sql", "--file", "run_sql/run_test.sql"])
    assert res.exit_code == 0
    assert "executing:" in res.output
    assert "select 1 as a" in res.output


def test_sql_files():
    res = run(["sql", "--files", "run_sql"])
    assert res.exit_code == 0
    print(res.output)
    assert "executing:" in res.output
    assert "select 1 as a" in res.output


def test_sql_file_failing_sql():
    res = run(["sql", "--file", "failing/run_sql/invalid.sql"])
    assert res.exit_code == 1


def test_sql_file_no_file():
    res = run(["sql", "--file", "failing/run_sql/no_such_file.sql"])
    assert res.exit_code == 1


def test_sql():
    res = run(["sql", "select 1 as a"])
    assert res.exit_code == 0
    assert res.output.strip() == f"a{linesep}1"


def test_sql_output():
    test_output = Path("test_sql_output_1.csv")
    if test_output.exists():
        test_output.unlink()
    res = run(["sql", "select 1 as a", "--output", str(test_output)])
    assert res.exit_code == 0
    exists = test_output.exists()
    test_output_data = test_output.read_text()
    test_output.unlink()
    assert test_output_data.strip() == "a\n1"
    assert exists


def test_sql_output_o():
    test_output = Path("test_sql_output_2.csv")
    if test_output.exists():
        test_output.unlink()
    res = run(["sql", "select 1 as a", "-o", str(test_output)])
    assert res.exit_code == 0
    exists = test_output.exists()
    test_output_data = test_output.read_text()
    test_output.unlink()
    assert test_output_data.strip() == "a\n1"
    assert exists


def test_sql_invalid_query():
    res = run(["sql", "selct 1 as a"])
    assert res.exit_code == 1


def test_quiet():
    res = run(["ping", "--quiet"])
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
    res = run(["sql", "select 1 as a", "--write-check", str(check_sql)])
    assert res.exit_code == 0
    assert check_sql.exists()
    assert check_csv.exists()
    assert check_sql.read_text() != ""
    assert check_csv.read_text() != ""


def test_write_check_W(tmp_path: Path):
    check_sql = tmp_path / "write_check.sql"
    check_csv = tmp_path / "write_check.csv"
    res = run(["sql", "select 1 as a", "-W", str(check_sql)])
    assert res.exit_code == 0
    assert check_sql.exists()
    assert check_csv.exists()
    assert check_sql.read_text() != ""
    assert check_csv.read_text() != ""


def test_log_is_written_in_project_path(tmp_path: Path):
    """This test runs data_check in a subfolder of the project path.
    The log file must still be written in the project path.
    """

    runner = CliRunner()
    workers_cmd = ["-n", str(1)]
    command = ["run", "-c", "test"]

    with runner.isolated_filesystem(tmp_path) as td:  # type: ignore
        tdp = Path(td)
        checks_path = tdp / "checks"
        checks_path.mkdir()

        Path(tdp / "data_check.yml").write_text(
            """
    connections:
        test: sqlite+pysqlite://

    log: dc.log
    """
        )

        runner.invoke(cli, command + workers_cmd)

        log_file = Path(tdp) / "dc.log"
        assert log_file.exists()


def test_failing_prints_no_setting_with_copy_warning():
    with warnings.catch_warnings(record=True) as record:
        run(["checks/failing/diff.sql"])
        for i in record:
            print(i)
        assert not record


def test_run_pipeline_yaml():
    res_folder = run(["run", "checks/pipelines/simple_pipeline"])
    res_file = run(["run", "checks/pipelines/simple_pipeline/data_check_pipeline.yml"])

    assert res_file
    assert res_folder
    # Currently there is a bug with nested pipelines
    # being run in parallel/non deterministically, even with a single worker.
    # Hence, we can only compare the length of the output, not the output itself.
    assert len(res_folder.output) == len(res_file.output)

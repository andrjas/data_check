from os import linesep
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


def test_sql_uses_lookups():
    res = run(
        [
            "sql",
            """with dat as (select 1 as a, 'a' as b {{from_dual}}
            union all
            select 2 as a, 'b' as b {{from_dual}}) select a from dat where b in :b1""",
        ]
    )
    assert res.exit_code == 0
    assert f"a{linesep}2" in res.output


def test_sql_files_uses_lookups():
    res = run(["sql", "--files", "checks/templates/binding.sql"])
    assert res.exit_code == 0
    assert "executing:" in res.output
    assert f"a{linesep}1{linesep}2" in res.output


def test_sql_with_output_does_not_print_on_console(tmp_path: Path):
    out_file = tmp_path / "a.sql"
    res = run(
        ["sql", "select 1 as output_test {{from_dual}}", "--output", str(out_file)]
    )
    assert res.exit_code == 0
    assert "output_test" not in res.output

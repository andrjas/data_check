from click.testing import CliRunner, Result
from os import linesep
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


def test_sql_uses_lookups():
    res = run(
        [
            "--sql",
            """with dat as (select 1 as a, 'a' as b {{from_dual}} union all select 2 as a, 'b' as b {{from_dual}}) select a from dat where b in :b1""",
        ]
    )
    assert res.exit_code == 0
    assert f"a{linesep}2" in res.output


def test_sql_files_uses_lookups():
    res = run(["--sql-files", "checks/templates/binding.sql"])
    assert res.exit_code == 0
    assert "executing:" in res.output
    assert f"a{linesep}1{linesep}2" in res.output

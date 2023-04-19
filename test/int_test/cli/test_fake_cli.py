import shutil
from os import sep
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


def run_fake(command: List[str], workers: Optional[int] = 1) -> Result:
    return run(["fake"] + command, workers)


def write_dc_config(path: Path):
    Path(path / "data_check.yml").write_text(
        """
    default_connection: test
    connections:
        test: sqlite+pysqlite:///d.db
    """
    )


def test_run_fake_without_parameters():
    res = run_fake([])
    assert "No config file given to generate fakes." in res.output


def test_run_fake_simple_table(tmp_path: Path):
    fake_path = Path("fake").absolute()
    runner = CliRunner()

    with runner.isolated_filesystem(tmp_path) as td:  # type: ignore
        tdp = Path(td)
        write_dc_config(tdp)
        shutil.copytree(fake_path, tdp / "fake")

        runner.invoke(cli, ["sql", "--file", "fake/prepare_fake/sql"])
        assert Path("d.db").exists()
        runner.invoke(cli, ["load", "fake/prepare_fake/data"])
        res = runner.invoke(cli, ["fake", "fake/simple_table.yml"])
        assert f"fake from fake{sep}simple_table.yml" in res.output
        assert "fake written to main.simple_table.csv" in res.output
        assert Path("main.simple_table.csv").exists()
        assert Path("main.simple_table_2.csv").exists()
        assert Path("main.simple_table_3.csv").exists()
        assert Path("main.simple_table_4.csv").exists()
        assert Path("main.simple_table_5.csv").exists()


def test_run_fake_simple_table_without_table(tmp_path: Path):
    fake_path = Path("fake").absolute()
    runner = CliRunner()

    with runner.isolated_filesystem(tmp_path) as td:  # type: ignore
        tdp = Path(td)
        write_dc_config(tdp)
        shutil.copytree(fake_path, tdp / "fake")

        res = runner.invoke(cli, ["fake", "fake/simple_table.yml"])
        assert "table main.simple_table does not exist" in str(res.exception)


def test_run_fake_minimal_config(tmp_path: Path):
    fake_path = Path("fake").absolute()
    runner = CliRunner()

    with runner.isolated_filesystem(tmp_path) as td:  # type: ignore
        tdp = Path(td)
        write_dc_config(tdp)
        shutil.copytree(fake_path, tdp / "fake")

        runner.invoke(cli, ["sql", "--file", "fake/prepare_fake/sql"])
        assert Path("d.db").exists()
        res = runner.invoke(cli, ["fake", "fake/minimal_config.yml"])
        assert f"fake from fake{sep}minimal_config.yml" in res.output
        assert "fake written to main.simple_table.csv" in res.output


def test_run_fake_does_not_overwrite(tmp_path: Path):
    fake_path = Path("fake").absolute()
    runner = CliRunner()

    with runner.isolated_filesystem(tmp_path) as td:  # type: ignore
        tdp = Path(td)
        write_dc_config(tdp)
        shutil.copytree(fake_path, tdp / "fake")
        csv = Path("main.simple_table.csv")
        csv.write_text("test")

        runner.invoke(cli, ["sql", "--file", "fake/prepare_fake/sql"])
        res = runner.invoke(cli, ["fake", "fake/minimal_config.yml"])
        assert Path("main.simple_table.csv").exists()
        assert csv.read_text() == "test"
        assert "already exists" in res.output


def test_run_fake_force_overwrites(tmp_path: Path):
    fake_path = Path("fake").absolute()
    runner = CliRunner()

    with runner.isolated_filesystem(tmp_path) as td:  # type: ignore
        tdp = Path(td)
        write_dc_config(tdp)
        shutil.copytree(fake_path, tdp / "fake")
        csv = Path("main.simple_table.csv")
        csv.write_text("test")

        runner.invoke(cli, ["sql", "--file", "fake/prepare_fake/sql"])
        runner.invoke(cli, ["fake", "--force", "fake/minimal_config.yml"])
        assert Path("main.simple_table.csv").exists()
        assert csv.read_text() != "test"

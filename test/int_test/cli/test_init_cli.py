from pathlib import Path

from click.testing import CliRunner

from data_check.cli.main import cli


def test_init(tmp_path: Path):
    runner = CliRunner()
    with runner.isolated_filesystem(tmp_path) as td:  # type: ignore
        result = runner.invoke(cli, ["init", "."])

    assert result.exit_code == 0

    conf = Path(td) / "data_check.yml"
    assert conf.exists()

    checks_path = Path(td) / "checks"
    assert checks_path.exists()


def test_init_subpath(tmp_path: Path):
    runner = CliRunner()
    with runner.isolated_filesystem(tmp_path) as td:  # type: ignore
        result = runner.invoke(cli, ["init", "sp"])

    assert result.exit_code == 0

    conf = Path(td) / "sp" / "data_check.yml"
    assert conf.exists()


def test_init_doesnt_overwrite_existing_config(tmp_path: Path):
    runner = CliRunner()
    with runner.isolated_filesystem(tmp_path) as td:  # type: ignore
        conf = Path(td) / "data_check.yml"
        conf.write_text("test")
        result = runner.invoke(cli, ["init", "."])

    assert result.exit_code == 1

    assert conf.exists()
    assert conf.read_text() == "test"

    checks_path = Path(td) / "checks"
    # the "checks" path is not created in this case, as the command failed
    assert not checks_path.exists()


def test_init_pipeline(tmp_path: Path):
    runner = CliRunner()
    with runner.isolated_filesystem(tmp_path) as td:  # type: ignore
        result = runner.invoke(cli, ["init", "--pipeline", "pp"])

    assert result.exit_code == 0

    pp = Path(td) / "pp"
    assert pp.exists()
    pp_conf = pp / "data_check_pipeline.yml"
    assert pp_conf.exists()


def test_init_pipeline_p(tmp_path: Path):
    runner = CliRunner()
    with runner.isolated_filesystem(tmp_path) as td:  # type: ignore
        result = runner.invoke(cli, ["init", "-p", "pp"])

    assert result.exit_code == 0

    pp = Path(td) / "pp"
    assert pp.exists()
    pp_conf = pp / "data_check_pipeline.yml"
    assert pp_conf.exists()


def test_init_pipeline_doesnt_overwrite(tmp_path: Path):
    runner = CliRunner()
    with runner.isolated_filesystem(tmp_path) as td:  # type: ignore
        pp = Path(td)
        pp_conf = pp / "data_check_pipeline.yml"
        pp_conf.write_text("test")
        result = runner.invoke(cli, ["init", "--pipeline", "."])

    assert result.exit_code == 1

    assert pp_conf.exists()
    assert pp_conf.read_text() == "test"

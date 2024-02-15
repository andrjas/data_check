from pathlib import Path

import pytest

from data_check.config import DataCheckConfig  # noqa E402

T_CONFIG = """
default_connection: t

connections:
    t: sqlite+pysqlite://
"""


def test_read_config():
    config = DataCheckConfig().load_config().config
    assert config
    assert "connections" in config
    connections = config.get("connections", {})
    assert "test" in connections
    assert "test2" in connections


def test_default_config_path():
    default_config_path = DataCheckConfig.config_path
    config = DataCheckConfig()
    assert config.config_path == default_config_path


def test_custom_config_path():
    config = DataCheckConfig(config_path=Path("test"))
    assert config.config_path == Path("test")


def test_select_connection():
    config = DataCheckConfig()
    config.config = {"connections": {"test": "data_check", "test2": "data_check2"}}
    selected, name = config.select_connection("test")
    assert selected == "data_check"
    assert name == "test"


def test_select_connection_default():
    config = DataCheckConfig()
    config.config = {
        "connections": {"test": "data_check", "test2": "data_check2"},
        "default_connection": "test2",
    }
    selected, name = config.select_connection(connection="")
    assert selected == "data_check2"
    assert name == "test2"


def test_select_connection_default_without_config():
    config = DataCheckConfig()
    config.config = {"connections": {"test": "data_check", "test2": "data_check2"}}
    selected, name = config.select_connection(connection="")
    assert selected == ""
    assert name == ""


def test_load_config_from_subpath(tmp_path: Path):
    cfg = tmp_path / "data_check.yml"
    cfg.write_text(T_CONFIG)
    sp = tmp_path / "subpath"
    sp.mkdir(parents=True, exist_ok=True)
    config = DataCheckConfig().load_config(base_path=sp)
    assert config.config.get("default_connection") == "t"


def test_load_config_from_subpath2(tmp_path: Path):
    cfg = tmp_path / "data_check.yml"
    cfg.write_text(T_CONFIG)
    sp = tmp_path / "subpath" / "sp2"
    sp.mkdir(parents=True, exist_ok=True)
    config = DataCheckConfig().load_config(base_path=sp)
    assert config.config.get("default_connection") == "t"


def test_load_config_no_config(tmp_path: Path):
    with pytest.raises(FileNotFoundError) as e:
        DataCheckConfig().load_config(base_path=tmp_path)
    assert "could not find " in str(e.value)


def test_project_path_is_parent_of_config(tmp_path: Path):
    cfg = tmp_path / "data_check.yml"
    cfg.write_text(T_CONFIG)
    config = DataCheckConfig().load_config(base_path=tmp_path)
    assert config.project_path == tmp_path


def test_project_path_is_parent_of_config_when_started_in_subpath(tmp_path: Path):
    cfg = tmp_path / "data_check.yml"
    cfg.write_text(T_CONFIG)
    sp = tmp_path / "subpath" / "sp2"
    sp.mkdir(parents=True, exist_ok=True)
    config = DataCheckConfig().load_config(base_path=sp)
    assert config.project_path == tmp_path


def test_template_path(tmp_path: Path):
    cfg = tmp_path / "data_check.yml"
    cfg.write_text(T_CONFIG)
    config = DataCheckConfig().load_config(tmp_path)
    assert (
        config.template_path.absolute()
        == tmp_path.absolute() / "checks" / "template.yml"
    )


def test_template_path_with_subdir(tmp_path: Path):
    cfg = tmp_path / "data_check.yml"
    cfg.write_text(T_CONFIG)
    sp = tmp_path / "subpath"
    sp.mkdir(parents=True, exist_ok=True)
    config = DataCheckConfig().load_config(base_path=sp)
    assert (
        config.template_path.absolute()
        == tmp_path.absolute() / "checks" / "template.yml"
    )


def test_project_path(tmp_path: Path):
    cfg = tmp_path / "data_check.yml"
    cfg.write_text(T_CONFIG)
    config = DataCheckConfig().load_config(tmp_path)
    assert config.project_path == tmp_path


def test_project_path_subdir(tmp_path: Path):
    cfg = tmp_path / "data_check.yml"
    cfg.write_text(T_CONFIG)
    sp = tmp_path / "subpath" / "sp2"
    sp.mkdir(parents=True, exist_ok=True)
    config = DataCheckConfig().load_config(base_path=sp)
    assert config.project_path == tmp_path


def test_checks_path(tmp_path: Path):
    cfg = tmp_path / "data_check.yml"
    cfg.write_text(T_CONFIG)
    config = DataCheckConfig().load_config(tmp_path)
    assert config.checks_path == tmp_path / "checks"


def test_checks_path_subdir(tmp_path: Path):
    cfg = tmp_path / "data_check.yml"
    cfg.write_text(T_CONFIG)
    sp = tmp_path / "subpath" / "sp2"
    sp.mkdir(parents=True, exist_ok=True)
    config = DataCheckConfig().load_config(base_path=sp)
    assert config.checks_path == sp


def test_base_path(tmp_path: Path):
    cfg = tmp_path / "data_check.yml"
    cfg.write_text(T_CONFIG)
    config = DataCheckConfig().load_config(tmp_path)
    assert config.base_path == tmp_path


def test_base_path_subdir(tmp_path: Path):
    cfg = tmp_path / "data_check.yml"
    cfg.write_text(T_CONFIG)
    sp = tmp_path / "subpath" / "sp2"
    sp.mkdir(parents=True, exist_ok=True)
    config = DataCheckConfig().load_config(base_path=sp)
    assert config.base_path == sp


def test_base_path_default():
    config = DataCheckConfig().load_config()
    assert config.base_path == Path().absolute()


def test_log_path_is_absolute(tmp_path: Path):
    cfg = tmp_path / "data_check.yml"
    cfg.write_text(T_CONFIG + "log: dc.log")
    config = DataCheckConfig().load_config(tmp_path)
    assert str(config.log_path) == str(tmp_path / "dc.log")

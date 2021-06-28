import sys
import os
from pathlib import Path

my_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, my_path + "/../")

from data_check.config import DataCheckConfig  # noqa E402


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
    selected = config.select_connection("test")
    assert selected == "data_check"


def test_select_connection_default():
    config = DataCheckConfig()
    config.config = {
        "connections": {"test": "data_check", "test2": "data_check2"},
        "default_connection": "test2",
    }
    selected = config.select_connection(connection="")
    assert selected == "data_check2"


def test_select_connection_default_without_config():
    config = DataCheckConfig()
    config.config = {"connections": {"test": "data_check", "test2": "data_check2"}}
    selected = config.select_connection(connection="")
    assert selected == ""

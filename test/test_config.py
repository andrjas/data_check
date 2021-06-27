import sys
import os

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

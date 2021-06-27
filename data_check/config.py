from pathlib import Path
from .io import read_yaml


class DataCheckConfig:
    config_path = Path("data_check.yml")
    checks_path = Path("checks")
    tempate_path = Path("template.yml")

    parallel_workers = 4

    default_print_format = "pandas"

    def __init__(self, config_path: Path = None) -> None:
        self.config = {}
        self.connection: str

        if config_path is not None:
            self.config_path = config_path

    def load_config(self):
        self.config = read_yaml(self.config_path)
        return self

    def set_connection(self, connection: str):
        self.connection = self.select_connection(connection)
        return self

    def select_connection(self, connection: str) -> str:
        """
        Returns the connection string to use.
        """
        if not connection:
            default_connection = self.config.get("default_connection")
            if default_connection:
                connection = default_connection

        return self.config.get("connections", {}).get(connection, "")

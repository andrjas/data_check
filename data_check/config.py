from pathlib import Path
from .io import read_yaml
from typing import Optional, Dict, Any, Tuple


class DataCheckConfig:
    config_path = Path("data_check.yml")
    checks_path = Path("checks")
    tempate_path = Path("template.yml")

    parallel_workers = 4

    default_print_format = "pandas"

    def __init__(self, config_path: Optional[Path] = None) -> None:
        self.config: Dict[str, Any] = {}
        self.connection: str
        self.connection_name: str

        if config_path is not None:
            self.config_path = config_path

        self.project_path = Path(".").absolute()

    def load_config(self):
        self.config = read_yaml(self.config_path)
        return self

    def set_connection(self, connection: str):
        self.connection, self.connection_name = self.select_connection(connection)
        return self

    def select_connection(self, connection: str) -> Tuple[str, str]:
        """
        Returns the connection string to use and the chosen connection name.
        """
        if not connection:
            default_connection = str(self.config.get("default_connection", ""))
            if default_connection:
                connection = default_connection

        return self.config.get("connections", {}).get(connection, ""), connection

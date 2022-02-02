from pathlib import Path
from .io import read_yaml
from typing import Optional, Dict, Any, Tuple


TEMPLATE_FILE = "template.yml"
CHECKS_PATH = "checks"
LOOKUPS_PATH = "lookups"


class DataCheckConfig:
    config_path = Path("data_check.yml")

    parallel_workers = 4

    default_print_format = "csv"

    def __init__(self, config_path: Optional[Path] = None) -> None:
        self.config: Dict[str, Any] = {}
        self.connection: Optional[str] = None
        self.connection_name: Optional[str] = None

        if config_path is not None:
            self.config_path = config_path

        self.project_path = Path(".").absolute()
        self.base_path = Path(".").absolute()

        self.generate_mode = False
        self.force = False
        self.log_path: Optional[Path] = None

    @property
    def print_overall_result(self):
        """When to print the overall result on console."""
        return not self.generate_mode

    @property
    def checks_path(self) -> Path:
        """Returns CHECKS_PATH if data_check is started from the project folder,
        otherwise it returns the base_path (pwd).

        This is NOT the default path for the "checks" folder (this is: CHECKS_PATH),
        but the path where data_check should start looking for checks
        if none are given on the command line.
        """
        if self.project_path == self.base_path:
            return self.project_path / CHECKS_PATH
        else:
            return self.base_path

    @property
    def template_path(self) -> Path:
        return self.project_path / CHECKS_PATH / TEMPLATE_FILE

    @property
    def lookups_path(self) -> Path:
        return self.project_path / LOOKUPS_PATH

    def find_config(self, base_path: Path) -> Path:
        abs_base_path = base_path.absolute()
        config_path = abs_base_path / self.config_path
        if not config_path.exists():
            if abs_base_path.parent == abs_base_path:
                raise Exception(f"could not find {self.config_path} in {abs_base_path}")
            return self.find_config(abs_base_path.parent)
        return abs_base_path / self.config_path

    def load_config(self, base_path: Path = Path(".")):
        try:
            config_path = self.find_config(base_path)
        except Exception:
            # raise basically the same exception as in find_config
            # but with base_path for better debugging
            raise Exception(
                f"could not find {self.config_path} in {base_path.absolute()}"
            )

        # project_path is always the directory where the config file is stored
        self.project_path = config_path.parent
        self.base_path = base_path.absolute()
        self.config_path = config_path
        self.config = read_yaml(config_path)
        _log_path = self.config.get("log", None)
        self.log_path = Path(_log_path) if _log_path else None
        if self.log_path and not self.log_path.is_absolute():
            self.log_path = self.project_path / self.log_path
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

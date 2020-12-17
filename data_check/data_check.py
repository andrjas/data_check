from pathlib import Path
import yaml


class DataCheck:
    """
    Main class for data_check.
    """

    @staticmethod
    def read_config(config_path: Path = Path("data_check.yml")):
        config = yaml.safe_load(config_path.open())
        return config


from pathlib import Path

from data_check.sql import DataCheckSql
from .fake_config import FakeConfig


def fake_from_config(
    config: Path,
    sql: DataCheckSql,
    output: Path = Path(),
    force=False,
    base_path: Path = Path("."),
):
    print("fake from", config)
    fake_config = FakeConfig(config)
    fake_config.parse()
    fake_config.init(sql)
    return fake_config.run_faker(output, force=force, base_path=base_path)

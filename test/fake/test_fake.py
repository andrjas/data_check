from pathlib import Path

import pytest
from sqlalchemy import Column, Integer, MetaData, String
from sqlalchemy import Table as SQLTable

from data_check import DataCheck  # noqa E402
from data_check.exceptions import TableDoesNotExistsException
from data_check.fake.fake_config import FakeConfig
from data_check.file_ops import read_csv
from data_check.sql.table import Table


def create_test_table_db(table_name: str, schema: str, dc: DataCheck):
    table = Table(dc.sql, table_name, schema)
    table.drop_if_exists()
    if dc.sql.dialect == "oracle":
        dc.sql.run_sql(f"create table {table} (id number(10), data varchar2(10))")
    else:
        metadata = MetaData()
        SQLTable(
            table.name,
            metadata,
            Column("id", Integer),
            Column("data", String(10)),
            schema=table.schema,
        )
        metadata.create_all(bind=dc.sql.get_engine())


def test_fake_config(tmp_path: Path, dc_serial: DataCheck):
    create_test_table_db("test_fake_config", "main", dc_serial)
    csv = tmp_path / "main.test_fake_config.csv"
    fake_config = FakeConfig(Path())
    config = {"table": "main.test_fake_config"}
    fake_config.load_config(config)
    fake_config.init(dc_serial.sql)
    fake_config.run_faker(csv)
    assert csv.exists()
    df = read_csv(csv)
    assert df.columns.tolist() == ["id", "data"]


def test_fake_config_other_path(tmp_path: Path, dc_serial: DataCheck):
    create_test_table_db("test_fake_config2", "main", dc_serial)
    csv = tmp_path / "main.test_fake_config2_other_path.csv"
    fake_config = FakeConfig(Path())
    config = {"table": "main.test_fake_config2"}
    fake_config.load_config(config)
    fake_config.init(dc_serial.sql)
    fake_config.run_faker(csv)
    assert csv.exists()


def test_fake_config_with_more_columns_fails(dc_serial: DataCheck):
    """This test fails, since we cannot query non existent columns"""
    create_test_table_db("test_fake_config_with_more_columns", "main", dc_serial)
    fake_config = FakeConfig(Path())
    config = {
        "table": "main.test_fake_config_with_more_columns",
        "columns": {
            "other_column": {"faker": "name"},
        },
    }
    fake_config.load_config(config)
    with pytest.raises(AttributeError):
        fake_config.init(dc_serial.sql)


def test_fake_config_iterations(tmp_path: Path, dc_serial: DataCheck):
    create_test_table_db("test_fake_config_iterations", "main", dc_serial)
    csv = tmp_path / "main.test_fake_config_iterations.csv"
    fake_config = FakeConfig(Path())
    config = {"table": "main.test_fake_config_iterations", "iterations": {"count": 3}}
    fake_config.load_config(config)
    fake_config.init(dc_serial.sql)
    fake_config.run_faker(csv)
    assert csv.exists()
    csv2 = tmp_path / "main.test_fake_config_iterations_2.csv"
    assert csv2.exists()
    csv3 = tmp_path / "main.test_fake_config_iterations_3.csv"
    assert csv3.exists()


def test_fake_config_non_existing_table(dc_serial: DataCheck):
    fake_config = FakeConfig(Path())
    config = {"table": "main.test_fake_config_non_existing"}
    fake_config.load_config(config)
    with pytest.raises(TableDoesNotExistsException):
        fake_config.init(dc_serial.sql)


def test_fake_config_iterations_wrong_next_strategy(
    tmp_path: Path, dc_serial: DataCheck
):
    create_test_table_db(
        "test_fake_config_iterations_wrong_next_strategy", "main", dc_serial
    )
    csv = tmp_path / "main.test_fake_config_iterations_wrong_next_strategy.csv"
    fake_config = FakeConfig(Path())
    config = {
        "table": "main.test_fake_config_iterations_wrong_next_strategy",
        "iterations": {"count": 3},
        "columns": {
            "data": {"next": "unknown_strategy"},
        },
    }
    fake_config.load_config(config)
    fake_config.init(dc_serial.sql)
    with pytest.raises(ValueError, match=r"unknown strategy: unknown_strategy"):
        fake_config.run_faker(csv)


def test_fake_pipeline(dc_serial: DataCheck):
    check = dc_serial.get_check(Path("checks/pipelines/fake_data"))
    assert check

    csv1 = Path("checks/pipelines/fake_data/main.simple_fake_table.csv")
    csv2 = Path("checks/pipelines/fake_data/main.simple_fake_table_2.csv")

    result = check.run_test()

    csv1_exists = csv1.exists()
    csv2_exists = csv2.exists()

    if csv2.exists():
        csv2.unlink()

    assert result
    assert csv1_exists
    assert csv2_exists

from pathlib import Path
import pytest
from sqlalchemy import Table, Column, String, Integer, MetaData


from data_check import DataCheck  # noqa E402

from data_check.fake.fake_config import FakeConfig
from data_check import DataCheck  # noqa E402
from data_check.io import read_csv


def create_test_table_db(table_name: str, schema: str, dc: DataCheck):
    dc.sql.table_loader.drop_table_if_exists(table_name, schema)
    if dc.sql.dialect == "oracle":
        dc.sql.run_sql(
            f"create table {schema}.{table_name} (id number(10), data varchar2(10))"
        )
    else:
        metadata = MetaData(dc.sql.get_engine())
        Table(
            table_name,
            metadata,
            Column("id", Integer),
            Column("data", String(10)),
            schema=schema,
        )
        metadata.create_all()


def test_fake_config(tmp_path: Path, dc_serial: DataCheck):
    create_test_table_db("test_fake_config", "main", dc_serial)
    csv = tmp_path / "main.test_fake_config.csv"
    fake_config = FakeConfig(Path("."))
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
    fake_config = FakeConfig(Path("."))
    config = {"table": "main.test_fake_config2"}
    fake_config.load_config(config)
    fake_config.init(dc_serial.sql)
    fake_config.run_faker(csv)
    assert csv.exists()


def test_fake_config_with_more_columns_fails(dc_serial: DataCheck):
    """This test fails, since we cannot query non existent columns"""
    create_test_table_db("test_fake_config_with_more_columns", "main", dc_serial)
    fake_config = FakeConfig(Path("."))
    config = {
        "table": "main.test_fake_config_with_more_columns",
        "columns": {
            "other_column": {"faker": "name"},
        },
    }
    fake_config.load_config(config)
    with pytest.raises(AttributeError):
        fake_config.init(dc_serial.sql)


def test_fake_config_non_existing_table(dc_serial: DataCheck):
    fake_config = FakeConfig(Path("."))
    config = {"table": "main.test_fake_config_non_existing"}
    fake_config.load_config(config)
    with pytest.raises(Exception):
        fake_config.init(dc_serial.sql)

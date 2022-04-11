from pathlib import Path
from typing import Any, Dict
import yaml
from faker import Faker
import pandas as pd

from data_check.sql import DataCheckSql
from data_check.io import write_csv
from .column_config import ColumnConfig
from .fake_iterator import FakeIterator


class FakeConfig:
    def __init__(self, config: Path):
        self.config_file = config
        self.config: Dict[str, Any]
        self.table_name: str
        self.business_key: str
        self.rows: int
        self.columns: Dict[str, ColumnConfig] = {}
        self.faker = Faker()
        self.iterator = FakeIterator(fake_config=self)

    def parse(self):
        config = yaml.safe_load(self.config_file.read_text())
        self.load_config(config)

    def load_config(self, config: Dict[str, Any]):
        self.config = config
        self.table_name = self.config.get("table", "")
        self.business_key = self.config.get("business_key", [])
        self.rows = self.config.get("rows", 100)
        self.iterator.parse(self.config.get("iterations", {}))

        columns = self.config.get("columns", {})
        for name, val in columns.items():
            is_unique = name == self.business_key
            if val:
                col_conf = ColumnConfig(
                    faker=self.faker,
                    name=name,
                    next=val.get("next", ""),
                    faker_name=val.get("faker", ""),
                    faker_args=val.get("faker_args", {}),
                    from_query=val.get("from_query", ""),
                    values=val.get("values", []),
                    is_unique=is_unique,
                )
            else:
                col_conf = ColumnConfig(
                    faker=self.faker, name=name, is_unique=is_unique
                )
            self.columns[name] = col_conf

    def get_column_types(self, sql: DataCheckSql):
        return sql.table_loader.get_column_types(self.table_name)

    def add_columns_not_in_config(self, column_types: Dict[str, Any]):
        for column in column_types:
            if column not in self.columns.keys():
                is_unique = column == self.business_key
                col_conf = ColumnConfig(
                    faker=self.faker, name=column, is_unique=is_unique
                )
                self.columns[column] = col_conf

    def init(self, sql: DataCheckSql):
        schema, table = sql.table_loader._parse_table_name(self.table_name)
        if not sql.table_loader.table_exists(table_name=table, schema=schema):
            raise Exception(f"table {self.table_name} does not exist")
        Faker.seed()
        column_types = self.get_column_types(sql)
        self.add_columns_not_in_config(column_types)
        for column in self.columns.values():
            column.init(sql_type=column_types.get(column.name, None), sql=sql)

    def generate_row(self) -> Dict[str, Any]:
        row = {}
        for column in self.columns.values():
            row[column.name] = column.generate()
        return row

    def generate_file(self, output: Path):
        df = None
        for _ in range(self.rows):
            row = self.generate_row()
            if df is None:
                df = pd.DataFrame(row, index=[0])
            else:
                df_new = pd.DataFrame(row, index=[0])
                df = pd.concat([df, df_new])
        if df is not None:
            write_csv(df, output)
            print(f"fake written to {output}")

    def run_faker(self, output: Path, force=False, base_path: Path = Path(".")):
        if output == Path():
            output = Path(f"{self.table_name}.csv")
        abs_output = output if output.is_absolute() else base_path / output
        if abs_output.exists() and not force:
            print(f"{abs_output} already exists")
            return False
        self.generate_file(abs_output)
        self.iterator.iterate(abs_output)
        return True

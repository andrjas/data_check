import datetime
import decimal
import random
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from faker import Faker

from data_check.sql import DataCheckSql


@dataclass
class ColumnConfig:
    faker: Faker
    name: str
    next: str = ""
    faker_name: str = ""
    faker_args: Dict[str, Any] = field(default_factory=dict)
    from_query: str = ""
    values: List[Any] = field(default_factory=list)
    sql_type: Any = None
    python_type: Any = None
    fake_method: Optional[Callable[..., Any]] = None
    is_unique: bool = False
    unique_data: List = field(default_factory=list)
    add_values: List[Any] = field(default_factory=list)

    def load_config(self, config: Dict[str, Any]):
        self.next = config.get("next", "")
        self.faker_name = config.get("faker", "")
        self.faker_args = config.get("faker_args", {})
        self.from_query = config.get("from_query", "")
        self.values = config.get("values", [])
        self.add_values = config.get("add_values", [])

    def python_type_to_faker(self, python_type) -> Callable[..., Any]:
        type_mapping = {
            decimal.Decimal: "pydecimal",
            str: "pystr",
            datetime.date: "date_between",
            datetime.datetime: "date_time_between",
        }
        fake_provider = type_mapping.get(python_type, "pyint")
        return getattr(self.faker, fake_provider)

    def fake_from_values(self) -> Any:
        return random.choice(self.values)

    def prepare_fake_method(self, sql: DataCheckSql):
        if self.values:
            self.fake_method = self.fake_from_values
            self.faker_args = {}
            return

        if self.from_query:
            res = sql.run_query(self.from_query)
            self.values = [v[0] for v in res.values.tolist()]
            self.fake_method = self.fake_from_values
            self.faker_args = {}
            return

        if self.faker_name:
            self.fake_method = getattr(self.faker, self.faker_name)
            self.faker_args = self.get_default_args()
        else:
            self.fake_method = self.python_type_to_faker(self.python_type)
            self.faker_args = self.get_default_args()

    def get_default_args(self) -> Dict[str, Any]:
        if self.python_type == decimal.Decimal:
            precision = self.sql_type.precision or 5
            scale = self.sql_type.scale or 0
            if not self.faker_args.get("left_digits", None):
                # In SQL precision is the whole "length" of the decimal
                # including the scale.
                self.faker_args["left_digits"] = precision - scale
            if not self.faker_args.get("right_digits", None):
                self.faker_args["right_digits"] = scale
        elif self.python_type == str:
            length = self.sql_type.length or 10
            if self.fake_method == self.faker.pystr:
                self.faker_args["max_chars"] = length
        return self.faker_args

    def init(self, sql_type: Any, sql: DataCheckSql):
        self.sql_type = sql_type
        self.python_type = sql_type.python_type
        self.prepare_fake_method(sql)

    def generate(self) -> Any:
        if self.fake_method:
            data = self.fake_method(**self.faker_args)
            if self.add_values:
                values = [data, *self.add_values]
                data = random.choice(values)
            if self.is_unique:
                while data in self.unique_data:
                    data = self.fake_method(**self.faker_args)
                self.unique_data.append(data)
            return data
        return None

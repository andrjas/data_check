from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from .fake_config import FakeConfig

from data_check.file_ops import read_csv, write_csv

from .iterators import dec, inc, random


@dataclass
class FakeIterator:
    fake_config: FakeConfig
    count: int = 1
    data: pd.DataFrame = field(default_factory=pd.DataFrame)

    def load_config(self, config: dict[str, Any]):
        self.count = config.get("count", 1)

    def get_iter_path(self, base_file: Path, i: int) -> Path:
        return base_file.parent / (base_file.stem + f"_{i}" + base_file.suffix)

    def iterate(self, base_file: Path):
        self.data = read_csv(base_file)
        for i in range(1, self.count):
            self.generate_iteration()
            iter_path = self.get_iter_path(base_file, i + 1)
            print(f"write iteration {i+1} to {iter_path}")
            write_csv(self.data, output=iter_path)

    def generate_iteration(self):
        for column in self.fake_config.columns.values():
            if column.next:
                strategy = self.next_to_strategy(column.next)
                if strategy:
                    strategy(column, self.data)
                else:
                    raise ValueError(f"unknown strategy: {column.next}")

    def next_to_strategy(self, next):
        return {"inc": inc, "random": random, "dec": dec}.get(next)

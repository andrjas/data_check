import click
from pathlib import Path
from typing import Union, List, Optional
from dataclasses import dataclass, field

from data_check.data_check import DataCheck


@dataclass
class RunParams:
    data_check: DataCheck
    ctx: click.Context
    load_mode: str
    table: Optional[str] = None
    files: List[Union[str, Path]] = field(default_factory=list)
    output: Union[str, Path] = ""
    write_check: Union[str, Path] = ""

from pathlib import Path
from typing import Optional, Union

import click

from data_check.config import DataCheckConfig
from data_check.utils import scaffold

from .common import common_options


@click.command()
@common_options
@click.option(
    "--pipeline",
    "-p",
    is_flag=True,
    help="create a pipeline",
)
@click.argument("path", nargs=1, type=click.Path())
@click.pass_context
def init(
    ctx: click.Context,
    path: Union[str, Path],
    connection: str = "",
    workers: int = DataCheckConfig.parallel_workers,
    config: Union[str, Path] = DataCheckConfig.config_path,
    verbose: bool = False,
    traceback: bool = False,
    quiet: bool = False,
    log: Optional[Union[str, Path]] = None,
    pipeline: bool = False,
):
    "Create a data_check project or pipeline."
    if pipeline:
        print(f"create pipeline {path}")
        scaffold.create_pipeline(Path(path))
    else:
        print(f"create {path}")
        scaffold.create_project(Path(path))

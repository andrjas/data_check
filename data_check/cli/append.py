from pathlib import Path
from typing import List, Optional, Union

import click

from data_check.config import DataCheckConfig

from .common import common_options
from .load import load_cmd


@click.command()
@common_options
@click.option(
    "--table",
    type=str,
    help="table name to load data into",
)
@click.argument("files", nargs=-1, type=click.Path())
@click.pass_context
def append(
    ctx: click.Context,
    connection: str = "",
    workers: int = DataCheckConfig.parallel_workers,
    config: Union[str, Path] = DataCheckConfig.config_path,
    verbose: bool = False,
    traceback: bool = False,
    quiet: bool = False,
    log: Optional[Union[str, Path]] = None,
    table: Optional[str] = None,
    files: List[Union[str, Path]] = [],
):
    """Append data from files into tables."""
    load_cmd(
        ctx,
        connection,
        workers,
        config,
        verbose,
        traceback,
        quiet,
        log,
        table,
        mode="append",
        files=files,
    )

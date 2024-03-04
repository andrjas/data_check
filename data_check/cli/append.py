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
def append(  # noqa: PLR0913
    ctx: click.Context,
    connection: str = "",
    workers: int = DataCheckConfig.parallel_workers,
    use_process: bool = DataCheckConfig.use_process,
    config: Union[str, Path] = DataCheckConfig.config_path,
    verbose: bool = False,
    traceback: bool = False,
    quiet: bool = False,
    log: Optional[Union[str, Path]] = None,
    table: Optional[str] = None,
    files: Optional[List[Union[str, Path]]] = None,
):
    """Append data from files into tables."""
    if files is None:
        files = []
    load_cmd(
        ctx,
        connection,
        workers,
        use_process,
        config,
        verbose,
        traceback,
        quiet,
        log,
        table,
        mode="append",
        files=files,
    )

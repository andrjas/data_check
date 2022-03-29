import click
from pathlib import Path
from typing import Union, Optional

from data_check.data_check import DataCheck
from data_check.config import DataCheckConfig


from .common import common_options, get_data_check


@click.command()
@common_options
@click.pass_context
def ping(
    ctx: click.Context,
    connection: str = "",
    workers: int = DataCheckConfig.parallel_workers,
    config: Union[str, Path] = DataCheckConfig.config_path,
    verbose: bool = False,
    traceback: bool = False,
    quiet: bool = False,
    log: Optional[Union[str, Path]] = None,
):
    """Tries to connect to the database."""
    dc = get_data_check(
        ctx=ctx,
        connection=connection,
        workers=workers,
        config=config,
        verbose=verbose,
        traceback=traceback,
        quiet=quiet,
        log=log,
    )

    test = dc.sql.test_connection()
    if test:
        ctx.exit(0)
    else:
        ctx.exit(1)

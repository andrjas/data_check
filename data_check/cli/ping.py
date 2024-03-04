from pathlib import Path
from typing import Optional, Union

import click

from data_check.config import DataCheckConfig

from .common import common_options, get_data_check


@click.command()
@common_options
@click.option(
    "--wait",
    is_flag=True,
    help="retry and wait until timeout is reached",
)
@click.option(
    "--timeout",
    type=int,
    default=DataCheckConfig.wait_timeout,
    help=f"timeout for wait in seconds (default: {DataCheckConfig.wait_timeout})",
)
@click.option(
    "--retry",
    type=int,
    default=DataCheckConfig.wait_retry,
    help=f"retry for wait in seconds (default: {DataCheckConfig.wait_retry})",
)
@click.pass_context
def ping(  # noqa: PLR0913
    ctx: click.Context,
    connection: str = "",
    workers: int = DataCheckConfig.parallel_workers,
    use_process: bool = DataCheckConfig.use_process,
    config: Union[str, Path] = DataCheckConfig.config_path,
    verbose: bool = False,
    traceback: bool = False,
    quiet: bool = False,
    wait: bool = False,
    timeout: int = 5,
    retry: int = 1,
    log: Optional[Union[str, Path]] = None,
):
    """Tries to connect to the database."""
    dc = get_data_check(
        ctx=ctx,
        connection=connection,
        workers=workers,
        use_process=use_process,
        config=config,
        verbose=verbose,
        traceback=traceback,
        quiet=quiet,
        log=log,
    )

    test = dc.sql.test_connection(wait=wait, timeout=timeout, retry=retry)
    if test:
        ctx.exit(0)
    else:
        ctx.exit(1)

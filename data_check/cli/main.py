import click
from click_default_group import DefaultGroup
from pathlib import Path
from colorama import init
from typing import Union, Optional

from data_check.config import DataCheckConfig

from .run import run
from .sql import sql
from .generate import gen
from .ping import ping
from .load import load
from .fake import fake
from .common import common_options, init_common


@click.group(cls=DefaultGroup, default="run", default_if_no_args=True)
@common_options
@click.pass_context
def cli(
    ctx: click.Context,
    connection: str = "",
    workers: int = DataCheckConfig.parallel_workers,
    config: Union[str, Path] = DataCheckConfig.config_path,
    verbose: bool = False,
    traceback: bool = False,
    quiet: bool = False,
    log: Optional[Union[str, Path]] = None,
):
    init()  # init colorama
    init_common(
        ctx=ctx,
        connection=connection,
        workers=workers,
        config=config,
        verbose=verbose,
        traceback=traceback,
        quiet=quiet,
        log=log,
    )


cli.add_command(run)
cli.add_command(sql)
cli.add_command(gen)
cli.add_command(ping)
cli.add_command(load)
cli.add_command(fake)

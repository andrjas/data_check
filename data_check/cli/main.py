from pathlib import Path
from typing import Optional, Union

import click
import colorama
from click_default_group import DefaultGroup  # type: ignore

from data_check.config import DataCheckConfig

from .append import append
from .common import common_options, init_common
from .fake import fake
from .generate import gen
from .init import init
from .load import load
from .ping import ping
from .run import run
from .sql import sql


@click.group(cls=DefaultGroup, default="run", default_if_no_args=True)
@common_options
@click.pass_context
def cli(
    ctx: click.Context,
    connection: str = "",
    workers: int = DataCheckConfig.parallel_workers,
    use_process: bool = DataCheckConfig.use_process,
    config: Union[str, Path] = DataCheckConfig.config_path,
    verbose: bool = False,
    traceback: bool = False,
    quiet: bool = False,
    log: Optional[Union[str, Path]] = None,
):
    colorama.init()
    init_common(
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


cli.add_command(run)
cli.add_command(sql)
cli.add_command(gen)
cli.add_command(ping)
cli.add_command(load)
cli.add_command(append)
cli.add_command(fake)
cli.add_command(init)

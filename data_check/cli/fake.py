from pathlib import Path
from typing import Optional, Union

import click

from data_check.config import DataCheckConfig

from .common import common_options, get_data_check


@click.command()
@common_options
@click.option("--output", "-o", type=click.Path(), help="output path for the CSV file")
@click.option(
    "--force",
    is_flag=True,
    help="overwrite existing files",
)
@click.argument("configs", nargs=-1, type=str, required=False)
@click.pass_context
def fake(
    ctx: click.Context,
    connection: str = "",
    workers: int = DataCheckConfig.parallel_workers,
    config: Union[str, Path] = DataCheckConfig.config_path,
    verbose: bool = False,
    traceback: bool = False,
    quiet: bool = False,
    log: Optional[Union[str, Path]] = None,
    output: Union[str, Path] = "",
    force: bool = False,
    configs: str = "",
):
    """Generate test data."""
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

    dc.load_template()
    dc.load_lookups()

    if configs:
        path_list = [Path(f) for f in configs]
        if output:
            dc.fake_data(configs=path_list, output=Path(output), force=force)
        else:
            dc.fake_data(configs=path_list, force=force)
        ctx.exit(0)
    else:
        click.echo("No config file given to generate fakes.")
        ctx.exit(1)

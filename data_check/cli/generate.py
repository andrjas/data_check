from pathlib import Path
from typing import List, Optional, Union

import click

from data_check.config import DataCheckConfig

from .common import common_options, get_data_check


@click.command()
@common_options
@click.option(
    "--force",
    is_flag=True,
    help="overwrite existing files",
)
@click.argument("files", nargs=-1, type=click.Path())
@click.pass_context
def gen(  # noqa: PLR0913
    ctx: click.Context,
    connection: str = "",
    workers: int = DataCheckConfig.parallel_workers,
    use_process: bool = DataCheckConfig.use_process,
    config: Union[str, Path] = DataCheckConfig.config_path,
    verbose: bool = False,
    traceback: bool = False,
    quiet: bool = False,
    log: Optional[Union[str, Path]] = None,
    force: bool = False,
    files: Optional[List[Union[str, Path]]] = None,
):
    """Generate CSV files from query files."""
    if files is None:
        files = []
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

    dc.config.generate_mode = True
    dc.config.force = force

    if not files:
        files = [dc.config.checks_path]  # use default checks path if nothing is given
    path_list = [Path(f) for f in files]
    dc.load_template()
    dc.load_lookups()
    result = dc.run(path_list)
    if not result:
        ctx.exit(1)

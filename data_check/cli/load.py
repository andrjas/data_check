from pathlib import Path
from typing import List, Optional, Union

import click

from data_check.config import DataCheckConfig

from .common import common_options, get_data_check


def load_cmd(
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
    mode: str = "truncate",
    files: Optional[List[Union[str, Path]]] = None,
):
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

    if table:
        if len(files) != 1:
            click.echo("--table can only be used with a single file")
            ctx.exit(1)
        load_file = Path(files[0])
        dc.sql.table_loader.load_table_from_file(
            table=table,
            file=load_file,
            mode=mode,
        )
        ctx.exit(0)
    else:
        if not files:
            click.echo(load.get_help(ctx))
            ctx.exit(0)
        path_list = [Path(f) for f in files]
        dc.sql.table_loader.load_tables_from_files(path_list, mode=mode)
        ctx.exit(0)


@click.command()
@common_options
@click.option(
    "--table",
    type=str,
    help="table name to load data into",
)
@click.option(
    "--mode",
    type=str,
    default="truncate",
    help="how to load the table: truncate (default), append, replace or upsert",
)
@click.argument("files", nargs=-1, type=click.Path())
@click.pass_context
def load(
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
    mode: str = "truncate",
    files: Optional[List[Union[str, Path]]] = None,
):
    """Load data from files into tables."""
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
        mode,
        files,
    )

import click
from pathlib import Path
from typing import Union, List, Optional

from data_check.config import DataCheckConfig


from .common import common_options, get_data_check


@click.command()
@common_options
@click.option(
    "--file",
    "--files",
    "sql_files",
    type=click.Path(),
    multiple=True,
    help="run any SQL script in a list of SQL files",
)
@click.option("--output", "-o", type=click.Path(), help="output path for the result")
@click.option(
    "--write-check",
    "-W",
    type=click.Path(),
    help="create a check from the sql statement (SQL and CSV file)",
)
@click.argument("query", nargs=1, type=str, required=False)
@click.pass_context
def sql(
    ctx: click.Context,
    connection: str = "",
    workers: int = DataCheckConfig.parallel_workers,
    config: Union[str, Path] = DataCheckConfig.config_path,
    verbose: bool = False,
    traceback: bool = False,
    quiet: bool = False,
    log: Optional[Union[str, Path]] = None,
    output: Union[str, Path] = "",
    write_check: Union[str, Path] = "",
    sql_files: List[Union[str, Path]] = [],
    query: str = "",
):
    """Run SQL statements."""
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

    if sql_files:
        path_list = [Path(f) for f in sql_files]
        dc.run_sql_files(path_list)
        ctx.exit(0)
    else:
        if write_check:
            result = dc.write_check(query, Path(write_check))
        else:
            result = dc.run_sql_query(query, output=output)
        if result:
            ctx.exit(0)
        else:
            ctx.exit(1)

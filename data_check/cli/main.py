import click
from pathlib import Path
import sys
from colorama import init
from importlib_metadata import version
from typing import Union, List, Optional, cast

from data_check.data_check import DataCheck
from data_check.config import DataCheckConfig
from .params import RunParams
from .commands import (
    run_cmd,
    get_callback,
    ping_cmd,
    load_cmd,
    load_tables_cmd,
    sql_cmd,
    sql_files_cmd,
)


@click.command()
@click.option("--connection", "-c", type=str, help="connection to use")
@click.option(
    "--workers",
    "-n",
    type=int,
    default=DataCheckConfig.parallel_workers,
    help=(
        "parallel workers to run queries "
        f"(default: {DataCheckConfig.parallel_workers})"
    ),
)
@click.option("--print", "print_failed", is_flag=True, help="print failed results")
@click.option(
    "--print-format",
    "print_format",
    type=str,
    default=DataCheckConfig.default_print_format,
    help=(
        "format for printing failed results (pandas, csv, json); "
        f"default: {DataCheckConfig.default_print_format}"
    ),
)
@click.option(
    "--print-json",
    is_flag=True,
    help="shortcut for --print --print-format json",
)
@click.option(
    "--diff",
    "print_diffed",
    is_flag=True,
    help="print only the different columns for failed results",
)
@click.option(
    "--generate",
    "-g",
    "generate_expectations",
    is_flag=True,
    help="generate expectation files if they don't exist",
)
@click.option(
    "--force",
    is_flag=True,
    help="when set, --generate will overwrite files",
)
@click.option(
    "--config",
    type=str,
    default=DataCheckConfig.config_path,
    help=f"config file to use (default: {DataCheckConfig.config_path})",
)
@click.option(
    "--load",
    type=click.Path(),
    help="load table data from a csv",
    callback=get_callback(load_cmd, True),
    expose_value=False,
)
@click.option(
    "--table",
    type=str,
    help="table name to load data into",
)
@click.option(
    "--load-mode",
    type=str,
    default="truncate",
    help="how to load the table: truncate (default), append or replace",
)
@click.option(
    "--load-tables",
    is_flag=True,
    help="load tables from a list of csv files",
    callback=get_callback(load_tables_cmd),
    expose_value=False,
)
@click.option(
    "--sql-file",
    "--sql-files",
    "sql_files",
    is_flag=True,
    help="run any SQL script in a list of SQL files",
    callback=get_callback(sql_files_cmd),
    expose_value=False,
)
@click.option(
    "--sql",
    type=str,
    help="run any SQL statement; print result as CSV if it is a query",
    callback=get_callback(sql_cmd, True),
    expose_value=False,
)
@click.option("--output", "-o", type=click.Path(), help="output path for --sql")
@click.option(
    "--write-check",
    "-W",
    type=click.Path(),
    help="create a check from the --sql statement (SQL and CSV file)",
)
@click.option(
    "--ping",
    is_flag=True,
    help="tries to connect to the database",
    callback=get_callback(ping_cmd),
    expose_value=False,
)
@click.option("--verbose", is_flag=True, help="print verbose output")
@click.option("--traceback", is_flag=True, help="print traceback output for debugging")
@click.option("--quiet", is_flag=True, help="do not print any output")
@click.option("--log", type=click.Path(), help="write output to a log file")
@click.version_option(version=cast(str, version("data-check")))
@click.argument("files", nargs=-1, type=click.Path())
@click.pass_context
def main(
    ctx: click.Context,
    connection: str = "",
    workers: int = DataCheckConfig.parallel_workers,
    print_failed: bool = False,
    print_format: str = DataCheckConfig.default_print_format,
    print_json: bool = False,
    print_diffed: bool = False,
    generate_expectations: bool = False,
    force: bool = False,
    config: Union[str, Path] = DataCheckConfig.config_path,
    table: Optional[str] = None,
    load_mode: str = "truncate",
    output: Union[str, Path] = "",
    write_check: Union[str, Path] = "",
    verbose: bool = False,
    traceback: bool = False,
    quiet: bool = False,
    log: Optional[Union[str, Path]] = None,
    files: List[Union[str, Path]] = [],
):
    """FILES: list of checks files or folders"""
    if print_json:
        print_failed = True
        print_format = "json"

    init()  # init colorama
    config = Path(config)
    dc_config = (
        DataCheckConfig(config_path=config).load_config().set_connection(connection)
    )
    dc_config.generate_mode = generate_expectations
    dc_config.force = force
    if log:
        dc_config.log_path = Path(log)

    if not dc_config.connection:
        click.echo(f"unknown connection: {connection}")
        sys.exit(1)

    dc_config.parallel_workers = workers

    dc = DataCheck(config=dc_config)
    dc.output.configure_output(
        verbose=verbose,
        traceback=traceback,
        print_failed=print_failed,
        print_format=print_format,
        print_diffed=print_diffed,
        quiet=quiet,
        # set log_path from config, so we can also use it from the config file
        log_path=dc_config.log_path,
        printer=click.echo,
    )
    res = RunParams(
        data_check=dc,
        ctx=ctx,
        table=table,
        load_mode=load_mode,
        files=files,
        write_check=write_check,
        output=output,
    )
    if ctx.obj:
        ctx.obj(res)
    else:
        run_cmd(res)

import click
from pathlib import Path
import sys
from colorama import init
from importlib_metadata import version
from typing import Union, List, Optional, cast

from data_check.data_check import DataCheck
from data_check.config import DataCheckConfig


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
    "--load-tables", is_flag=True, help="load tables from a list of csv files"
)
@click.option(
    "--sql-file",
    "--sql-files",
    "sql_files",
    is_flag=True,
    help="run any SQL script in a list of SQL files",
)
@click.option(
    "--sql",
    type=str,
    help="run any SQL statement; print result as CSV if it is a query",
)
@click.option("--output", "-o", type=click.Path(), help="output path for --sql")
@click.option("--ping", is_flag=True, help="tries to connect to the database")
@click.option("--verbose", is_flag=True, help="print verbose output")
@click.option("--traceback", is_flag=True, help="print traceback output for debugging")
@click.option("--quiet", is_flag=True, help="do not print any output")
@click.option("--log", type=click.Path(), help="write output to a log file")
@click.version_option(version=cast(str, version("data-check")))
@click.argument("files", nargs=-1, type=click.Path())
def main(
    connection: str = "",
    workers: int = DataCheckConfig.parallel_workers,
    print_failed: bool = False,
    print_format: str = DataCheckConfig.default_print_format,
    print_json: bool = False,
    generate_expectations: bool = False,
    force: bool = False,
    config: Union[str, Path] = DataCheckConfig.config_path,
    load: Optional[Path] = None,
    table: Optional[str] = None,
    load_mode: str = "truncate",
    load_tables: bool = False,
    sql_files: bool = False,
    sql: str = "",
    output: Union[str, Path] = "",
    ping: bool = False,
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
        quiet=quiet,
        # set log_path from config, so we can also use it from the config file
        log_path=dc_config.log_path,
    )

    if load:
        if not table:
            click.echo("--table must be specified")
            sys.exit(1)
        else:
            dc.sql.table_loader.load_table_from_file(
                table=table,
                file=load,
                load_mode=load_mode,
            )
            sys.exit(0)
    elif load_tables:
        path_list = [Path(f) for f in files]
        dc.sql.table_loader.load_tables_from_files(path_list, load_mode=load_mode)
        sys.exit(0)

    dc.load_template()

    if sql_files:
        dc.load_lookups()
        path_list = [Path(f) for f in files]
        dc.run_sql_files(path_list)
        sys.exit(0)

    if sql:
        dc.load_lookups()
        if dc.run_sql_query(sql, output=output):
            sys.exit(0)
        else:
            sys.exit(1)

    if not files:
        files = [dc_config.checks_path]  # use default checks path if nothing is given
    path_list = [Path(f) for f in files]

    if ping:
        test = dc.sql.test_connection()
        if test:
            sys.exit(0)
        else:
            sys.exit(1)
    else:
        dc.load_lookups()
        result = dc.run(path_list)
        if not result:
            sys.exit(1)


if __name__ == "__main__":
    main()

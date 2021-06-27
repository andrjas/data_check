import click
from pathlib import Path
import sys
from colorama import init
from importlib_metadata import version

from data_check.data_check import DataCheck
from data_check.config import DataCheckConfig


HELP_WORKERS_LINE = (
    "parallel workers to run queries " f"(default: {DataCheckConfig.parallel_workers})"
)

HELP_FORMAT_LINE = (
    "format for printing failed results (pandas, csv); "
    f"default: {DataCheckConfig.default_print_format}"
)


@click.command()
@click.option("--connection", "-c", type=str, help="connection to use")
@click.option(
    "--workers",
    "-n",
    type=int,
    default=DataCheckConfig.parallel_workers,
    help=HELP_WORKERS_LINE,
)
@click.option("--print", "print_failed", is_flag=True, help="print failed results")
@click.option(
    "--print-format",
    "print_format",
    type=str,
    default=DataCheckConfig.default_print_format,
    help=HELP_FORMAT_LINE,
)
@click.option(
    "--print-csv",
    "print_csv",
    is_flag=True,
    help="shortcut for --print --print-format csv",
)
@click.option(
    "--gen",
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
@click.option("--ping", is_flag=True, help="tries to connect to the database")
@click.option("--verbose", is_flag=True, help="print verbose output")
@click.option("--traceback", is_flag=True, help="print traceback output for debugging")
@click.version_option(version=version("data-check"))
@click.argument("files", nargs=-1, type=click.Path())
def main(
    connection=None,
    workers=DataCheckConfig.parallel_workers,
    print_failed=False,
    print_format=DataCheckConfig.default_print_format,
    print_csv=False,
    generate_expectations=False,
    force=False,
    config=DataCheckConfig.config_path,
    ping=False,
    verbose=False,
    traceback=False,
    files=[],
):
    """ FILES: list of checks files or folders"""

    if print_csv:
        print_failed = True
        print_format = "csv"

    init()  # init colorama
    config = Path(config)
    dc_config = (
        DataCheckConfig(config_path=config).load_config().set_connection(connection)
    )

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
    )
    dc.load_template()

    if not files:
        files = [dc_config.checks_path]  # use default checks path if nothing is given
    path_list = [Path(f) for f in files]

    if generate_expectations:
        dc.generator.generate_expectations(path_list, force)
    elif ping:
        test = dc.sql.test_connection()
        if test:
            click.echo("connecting succeeded")
            sys.exit(0)
        else:
            click.echo("connecting failed")
            sys.exit(1)
    else:
        result = dc.run(path_list)
        if not result:
            sys.exit(1)


if __name__ == "__main__":
    main()

import click
from pathlib import Path
import sys
from colorama import init
from importlib_metadata import version

from data_check.data_check import DataCheck


def select_connection(connection, config) -> str:
    """
    Returns the connection string to use.
    """
    if not connection:
        default_connection = config.get("default_connection")
        if default_connection:
            connection = default_connection

    return config.get("connections", {}).get(connection)


@click.command()
@click.option("--connection", "-c", type=str, help="connection to use")
@click.option(
    "--workers",
    "-n",
    type=int,
    default=4,
    help="parallel workers to run queries (default: 4)",
)
@click.option("--print", "print_failed", is_flag=True, help="print failed results")
@click.option(
    "--print-format",
    "print_format",
    type=str,
    default="pandas",
    help="format for printing failed results (pandas, csv)",
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
    default="data_check.yml",
    help="config file to use (default: data_check.yml)",
)
@click.option("--ping", is_flag=True, help="tries to connect to the database")
@click.option("--verbose", is_flag=True, help="print verbose output")
@click.option("--traceback", is_flag=True, help="print traceback output for debugging")
@click.version_option(version=version("data-check"))
@click.argument("files", nargs=-1, type=click.Path())
def main(
    connection=None,
    workers=4,
    print_failed=False,
    print_format="pandas",
    print_csv=False,
    generate_expectations=False,
    force=False,
    config="data_check.yml",
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

    config = DataCheck.read_config(Path(config))
    selected_connection = select_connection(connection, config)

    if not selected_connection:
        click.echo(f"unknown connection: {connection}")
        sys.exit(1)

    dc = DataCheck(
        connection=selected_connection,
        workers=workers,
        verbose=verbose,
        traceback=traceback,
    )
    dc.load_template()

    if not files:
        files = ["checks"]  # use "checks" if nothing is given
    path_list = [Path(f) for f in files]

    if generate_expectations:
        dc.generate_expectations(path_list, force)
    elif ping:
        test = dc.test_connection()
        if test:
            click.echo("connecting succeeded")
            sys.exit(0)
        else:
            click.echo("connecting failed")
            sys.exit(1)
    else:
        result = dc.run(path_list, print_failed=print_failed, print_format=print_format)
        if not result:
            sys.exit(1)


if __name__ == "__main__":
    main()

import click
from pathlib import Path
import sys

from data_check.data_check import DataCheck
from data_check import __version__ as data_check_version


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
    "--gen",
    "--generate",
    "generate_expectations",
    is_flag=True,
    help="generate expectation files if they don't exist",
)
@click.option(
    "--config",
    type=str,
    default="data_check.yml",
    help="config file to use (default: data_check.yml)",
)
@click.option("--ping", is_flag=True, help="tries to connect to the database")
@click.version_option(version=data_check_version)
@click.argument("files", nargs=-1, type=click.Path())
def main(
    connection=None,
    workers=4,
    print_failed=False,
    generate_expectations=False,
    config="data_check.yml",
    ping=False,
    files=[],
):
    """ FILES: list of checks files or folders"""

    config = DataCheck.read_config(Path(config))
    selected_connection = select_connection(connection, config)

    dc = DataCheck(connection=selected_connection, workers=workers)

    if not files:
        files = ["checks"]  # use "checks" if nothing is given
    path_list = [Path(f) for f in files]

    if generate_expectations:
        dc.generate_expectations(path_list)
    elif ping:
        test = dc.test_connection()
        if test:
            click.echo("connecting succeeded")
            sys.exit(0)
        else:
            click.echo("connecting failed")
            sys.exit(1)
    else:
        result = dc.run(path_list, print_failed=print_failed)
        if not result:
            sys.exit(1)


if __name__ == "__main__":
    main()

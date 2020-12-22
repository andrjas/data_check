import argparse
from pathlib import Path
import sys

from data_check.data_check import DataCheck


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="data_check")

    parser.add_argument("-c", "--connection", type=str, help="connection to use")
    parser.add_argument(
        "-n", "--workers", type=int, default=4, help="parallel workers to run queries (default: 4)"
    )
    parser.add_argument(
        "--print", action="store_true", dest="print_failed", help="print failed results"
    )
    parser.add_argument(
        "--gen",
        "--generate",
        dest="generate_expectations",
        action="store_true",
        help="generate expectation files if they don't exist",
    )
    parser.add_argument(
        "--config", type=str, default="data_check.yml", help="config file to use (default: data_check.yml)"
    )
    parser.add_argument(
        "files",
        metavar="files",
        type=str,
        nargs="*",
        default=["checks"],
        help="list of checks files or folders",
    )
    return parser.parse_args()


def select_connection(connection, config) -> str:
    """
    Returns the connection string to use.
    """
    if not connection:
        default_connection = config.get("default_connection")
        if default_connection:
            connection = default_connection

    return config.get("connections", {}).get(connection)


def main():
    args = parse_args()
    config = DataCheck.read_config(Path(args.config))

    selected_connection = select_connection(args.connection, config)

    dc = DataCheck(connection=selected_connection, workers=args.workers)
    path_list = [Path(f) for f in args.files]

    if args.generate_expectations:
        dc.generate_expectations(path_list)
    else:
        result = dc.run(path_list, print_failed=args.print_failed)
        if not result:
            sys.exit(1)


if __name__ == "__main__":
    main()

import argparse
from pathlib import Path
import sys

from data_check.data_check import DataCheck


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="data_check")

    parser.add_argument("-c", "--connection", type=str, help="connection to use")
    parser.add_argument(
        "-n", "--workers", type=int, default=4, help="parallel workers to run queries"
    )
    parser.add_argument(
        "--print", action="store_true", dest="print_failed", help="print failed results"
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
    config = DataCheck.read_config()

    selected_connection = select_connection(args.connection, config)

    dc = DataCheck(connection=selected_connection, workers=args.workers)
    result = dc.run([Path(f) for f in args.files], print_failed=args.print_failed)
    if not result:
        sys.exit(1)


if __name__ == "__main__":
    main()

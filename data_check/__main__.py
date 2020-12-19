import argparse
from pathlib import Path

from data_check.data_check import DataCheck


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="data_check")

    parser.add_argument("-c", "--connection", type=str, help="connection to use")

    parser.add_argument(
        "files",
        metavar="files",
        type=str,
        nargs="*",
        help="list of checks files or folders",
    )
    return parser.parse_args()


def select_connection(connection, config) -> str:
    """
    Returns the connection string to use.
    """
    return config.get("connections", {}).get(connection)


def main():
    args = parse_args()
    config = DataCheck.read_config()

    selected_connection = select_connection(args.connection, config)

    dc = DataCheck(selected_connection)
    for f in args.files:
        dc.run(Path(f))


if __name__ == "__main__":
    main()

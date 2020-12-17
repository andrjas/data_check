import argparse


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="data_check")

    return parser.parse_args()


def main():
    args = parse_args()


if __name__ == "__main__":
    main()

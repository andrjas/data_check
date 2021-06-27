import pandas as pd
from colorama import Fore, Style
from .exceptions import DataCheckException


def pprint_failed(df: pd.DataFrame, print_format="pandas") -> str:
    """
    Prints a DataFrame with diff information and returns it as a string.
    """
    with pd.option_context("display.max_rows", None, "display.max_columns", None):
        df["_diff"] = ""
        df.loc[df._merge == "left_only", ["_diff"]] = "db"
        df.loc[df._merge == "right_only", ["_diff"]] = "expected"
        if print_format == "pandas":
            return str(df.drop(["_merge"], axis=1))
        elif print_format.lower() == "csv":
            return df.drop(["_merge"], axis=1).to_csv(index=False)
        else:
            raise DataCheckException(f"unknown print format: {print_format}")


def str_pass(string):
    return Fore.GREEN + string + Style.RESET_ALL


def str_warn(string):
    return Fore.YELLOW + string + Style.RESET_ALL


def str_fail(string):
    return Fore.RED + string + Style.RESET_ALL

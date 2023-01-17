from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import yaml
from jinja2 import Template
from pandas._typing import DtypeArg
from pandas.core.frame import DataFrame

from .date import isoparse, parse_date_columns
from .exceptions import DataCheckException


def expand_files(
    files: List[Path],
    extension: Union[str, List[str]] = ".sql",
    base_path: Path = Path("."),
) -> List[Path]:
    """
    Expands the list of files or folders,
    with all SQL files in a folder as separate files.
    """
    if isinstance(extension, str):
        extensions = [extension]
    else:
        extensions = extension
    result: List[Path] = []
    for f in files:
        rel_file = base_path / f
        if rel_file.is_file():
            result.append(rel_file)
        elif rel_file.is_dir():
            for ext in extensions:
                result.extend(rel_file.glob(f"**/*{ext}"))
        else:
            raise Exception(f"unexpected path: {rel_file}")
    return sorted(result)


def parse_template(data: str, template_data: Dict[str, Any]) -> str:
    return Template(data).render(**template_data)


def read_sql_file(
    sql_file: Path, template_data: Dict[str, Any], encoding: str = "UTF-8"
) -> str:
    """
    Reads the SQL file and returns it as a string.
    Evaluates the templates when needed.
    """
    try:
        return parse_template(sql_file.read_text(encoding=encoding), template_data)
    except Exception as e:
        raise DataCheckException(f"Failed to read {sql_file}: {e}")


def get_expect_file(sql_file: Path) -> Path:
    """
    Returns the csv file with the expected results for a sql file.
    """
    if (
        str(sql_file) == ""
        or sql_file.stem == ""
        or sql_file.suffix == ""
        or sql_file.suffix.lower() not in (".sql")
    ):
        return Path()
    return sql_file.parent / (sql_file.stem + ".csv")


DEFAULT_READ_CSV_PARAMS = {
    "na_values": [""],  # use empty string as nan
    "keep_default_na": False,
    "comment": "#",
    "escapechar": "\\",
    "quotechar": '"',
    "quoting": 0,
    "engine": "c",
}


def read_csv_header(csv_file: Path) -> List[str]:
    """Read only the header of a CSV file and returns the list of columns."""
    try:
        df = pd.read_csv(csv_file, nrows=0, **DEFAULT_READ_CSV_PARAMS)
    except Exception as e:
        raise DataCheckException(f"Failed to read {csv_file}: {e}")
    return df.columns.tolist()


def read_csv(
    csv_file: Path,
    parse_dates: Union[bool, List[str]] = False,
    string_columns: List[str] = [],
) -> pd.DataFrame:
    """Reads a CSV file and returns a DataFrame with the data from the file.
    If parse_dates is given, .date.isoparse is used to convert the columns in parse_dates to datetime.
    If parse_dates is not given, .date.parse_date_columns is used to try to convert all columns to datetime.

    string_columns holds a list of all columns that should be treated as strings, without any convertion.
    """
    if not parse_dates:
        parse_dates = False
    dtypes: DtypeArg = {s: "object" for s in string_columns}

    # remove columns from parse_dates if they are not in the CSV file
    if isinstance(parse_dates, List):
        header = read_csv_header(csv_file)
        parse_dates = [p for p in parse_dates if p in header]

    try:
        df = pd.read_csv(
            csv_file,
            parse_dates=parse_dates,
            date_parser=isoparse,
            dtype=dtypes,
            **DEFAULT_READ_CSV_PARAMS,
        )
    except Exception as e:
        raise DataCheckException(f"Failed to read {csv_file}: {e}")

    if not parse_dates:
        _, df = parse_date_columns(df)
    return df


def print_csv(df: DataFrame, print_method):
    print_method(df.to_csv(index=False))


def write_csv(
    df: DataFrame, output: Union[str, Path] = "", base_path: Path = Path(".")
):
    if output:
        result = df.to_csv(index=False, lineterminator="\n")
        # escape # in strings that would otherwise be treated as the start of a comment
        result = result.replace("#", "\\#")
        Path(base_path / output).write_text(result, encoding="utf8")


def read_yaml(
    yaml_file: Path,
    encoding: str = "UTF-8",
    template_data: Optional[Dict[str, Any]] = None,
):
    data = yaml_file.read_text(encoding=encoding)
    if template_data:
        data = parse_template(data, template_data)
    return yaml.safe_load(data)


def rel_path(path: Path) -> Path:
    """
    Returns the path relative to where data_check is started from
    if it's relative to this path.
    """
    try:
        return path.absolute().relative_to(Path(".").absolute())
    except ValueError:
        return path

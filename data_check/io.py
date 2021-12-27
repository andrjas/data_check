from typing import List, Dict, Any, Union, Optional, cast
from pathlib import Path
from jinja2 import Template
import pandas as pd
from pandas.core.frame import DataFrame
import yaml

from .date import isoparse, parse_date_columns


def expand_files(
    files: List[Path],
    extension: Union[str, List[str]] = ".sql",
    base_path: Path = Path("."),
) -> List[Path]:
    """
    Expands the list of files or folders,
    with all SQL files in a folder as seperate files.
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
    return result


def parse_template(data: str, template_data: Dict[str, Any]) -> str:
    return Template(data).render(**template_data)


def read_sql_file(
    sql_file: Path, template_data: Dict[str, Any], encoding: str = "UTF-8"
) -> str:
    """
    Reads the SQL file and returns it as a string.
    Evaluates the templates when needed.
    """
    return parse_template(sql_file.read_text(encoding=encoding), template_data)


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
    dtypes = {s: "object" for s in string_columns}
    df = pd.read_csv(
        csv_file,
        na_values=[""],  # use empty string as nan
        keep_default_na=False,
        comment="#",
        escapechar="\\",
        quotechar='"',
        quoting=0,
        engine="c",
        parse_dates=parse_dates,
        date_parser=isoparse,
        dtype=dtypes,
    )
    if not parse_dates:
        _, df = parse_date_columns(df)
    return df


def print_csv(df: DataFrame, print_method):
    print_method(df.to_csv(index=False))


def write_csv(
    df: DataFrame, output: Union[str, Path] = "", base_path: Path = Path(".")
):
    if output:
        result = df.to_csv(index=False, line_terminator="\n")
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

from typing import List, Dict, Any, Union, Optional
from pathlib import Path
from jinja2 import Template
import pandas as pd
from pandas.core.frame import DataFrame
import yaml
import datetime


def expand_files(
    files: List[Path], extension: str = ".sql", base_path: Path = Path(".")
) -> List[Path]:
    """
    Expands the list of files or folders,
    with all SQL files in a folder as seperate files.
    """
    result: List[Path] = []
    for f in files:
        rel_file = base_path / f
        if rel_file.is_file():
            result.append(rel_file)
        elif rel_file.is_dir():
            result.extend(rel_file.glob(f"**/*{extension}"))
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
    string_columns: Dict[str, str] = {},
) -> pd.DataFrame:
    if not parse_dates:
        parse_dates = False
    dtypes = {s: "object" for s in string_columns}
    return pd.read_csv(
        csv_file,
        na_values=[""],  # use empty string as nan
        keep_default_na=False,
        comment="#",
        quotechar='"',
        quoting=0,
        engine="c",
        parse_dates=parse_dates,
        infer_datetime_format=True,
        dtype=dtypes,
    )


def print_csv(df: DataFrame):
    print(df.to_csv(index=False))


def write_csv(
    df: DataFrame, output: Union[str, Path] = "", base_path: Path = Path(".")
):
    if output:
        df.to_csv(base_path / output, index=False)


def read_yaml(
    yaml_file: Path,
    encoding: str = "UTF-8",
    template_data: Optional[Dict[str, Any]] = None,
):
    data = yaml_file.read_text(encoding=encoding)
    if template_data:
        data = parse_template(data, template_data)
    return yaml.safe_load(data)

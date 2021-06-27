from typing import List, Dict, Any
from pathlib import Path
from jinja2 import Template
import pandas as pd
import yaml


def expand_files(files: List[Path]) -> List[Path]:
    """
    Expands the list of files or folders,
    with all SQL files in a folder as seperate files.
    """
    result: List[Path] = []
    for f in files:
        if f.is_file():
            result.append(f)
        elif f.is_dir():
            result.extend(f.glob("**/*.sql"))
        else:
            raise Exception(f"unexpected path: {f}")
    return result


def read_sql_file(
    sql_file: Path, template_data: Dict[str, Any], encoding: str = "UTF-8"
) -> str:
    """
    Reads the SQL file and returns it as a string.
    Evaluates the templates when needed.
    """
    return Template(sql_file.read_text(encoding=encoding)).render(**template_data)


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


def read_csv(csv_file: Path) -> pd.DataFrame:
    return pd.read_csv(
        csv_file,
        na_values=[""],  # use empty string as nan
        keep_default_na=False,
        comment="#",
        quotechar='"',
        quoting=0,
        engine="c",
    )


def read_yaml(yaml_file: Path):
    return yaml.safe_load(yaml_file.open())

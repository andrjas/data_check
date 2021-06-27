from typing import List
from pathlib import Path
from jinja2 import Template


def expand_files(files: List[Path]) -> List[Path]:
    """
    Expands the list of files or folders,
    with all SQL files in a folder as seperate files.
    """
    result = []
    for f in files:
        if f.is_file():
            result.append(f)
        elif f.is_dir():
            result.extend(f.glob("**/*.sql"))
        else:
            raise Exception(f"unexpected path: {f}")
    return result


def read_sql_file(sql_file, template_data, encoding="UTF-8") -> str:
    """
    Reads the SQL file and returns it as a string.
    Evaluates the templates when needed.
    """
    return Template(sql_file.read_text(encoding=encoding)).render(
        **template_data
    )

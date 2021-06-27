import sys
import os
from pathlib import Path

my_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, my_path + "/../")


from data_check.io import expand_files, read_sql_file, get_expect_file  # noqa E402


def test_expand_files():
    files = expand_files([Path("checks/basic"), Path("checks/failing")])
    assert len(files) >= 3


def test_read_sql_file_with_template():
    text = read_sql_file(Path("checks/templates/template1.sql"), {})
    assert "{{" not in text


def test_get_expect_file():
    ef = get_expect_file(Path("test_file.sql"))
    assert ef == Path("test_file.csv")


def test_get_expect_file_null():
    p = Path()
    ef = get_expect_file(p)
    assert ef == Path()


def test_get_expect_file_empty():
    p = Path("")
    ef = get_expect_file(p)
    assert ef == Path()


def test_get_expect_file_sql():
    p = Path(".sql")
    ef = get_expect_file(p)
    print(f"stem: {p.stem}")
    print(f"suffix: '{p.suffix}'")
    assert ef == Path()

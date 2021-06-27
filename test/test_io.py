import sys
import os
from pathlib import Path


my_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, my_path + "/../")


from data_check.data_check_io import expand_files, read_sql_file  # noqa E402


def test_expand_files():
    files = expand_files([Path("checks/basic"), Path("checks/failing")])
    assert len(files) >= 3


def test_read_sql_file_with_template():
    text = read_sql_file(Path("checks/templates/template1.sql"), {})
    assert "{{" not in text

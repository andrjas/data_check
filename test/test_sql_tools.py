import sys
import os

# add the parent path to PYTHONPATH
my_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, my_path + "/../")


from data_check.sql.tools import parse_date_hint


def test_parse_date_hint():
    dh = parse_date_hint(
        """-- date: dc1, dc2
select 1
"""
    )
    assert dh == ["dc1", "dc2"]


def test_parse_no_date_hint():
    dh = parse_date_hint("""select 1""")
    assert dh == []


def test_parse_date_hint_newline():
    dh = parse_date_hint(
        """
-- date: dc1, dc2
select 1
"""
    )
    assert dh == ["dc1", "dc2"]


def test_parse_date_hint_newline_sep():
    dh = parse_date_hint(
        """
   -- date: dc1, dc2
select 1
"""
    )
    assert dh == ["dc1", "dc2"]


def test_parse_date_hint_no_sep():
    dh = parse_date_hint(
        """--date:dc1,dc2
select 1
"""
    )
    assert dh == ["dc1", "dc2"]


def test_parse_multiple_date_hint():
    dh = parse_date_hint(
        """-- date: dc1, dc2
    -- date: dc3
select 1
"""
    )
    assert dh == ["dc1", "dc2", "dc3"]

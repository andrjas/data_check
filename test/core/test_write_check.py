from pathlib import Path

from data_check import DataCheck


def test_write_check(dc: DataCheck, tmp_path: Path):
    sql = "select 1 as a union all select 2 as a"
    sql_file = tmp_path / "write_check.sql"
    result = dc.write_check(sql, sql_file)
    csv_file = tmp_path / "write_check.csv"
    assert result
    assert sql_file.exists()
    assert csv_file.exists()
    assert sql_file.read_text() == sql
    assert csv_file.read_text().strip() == "a\n1\n2"


def test_write_check_existing_csv(dc: DataCheck, tmp_path: Path):
    sql = "select 1 as a union all select 2 as a"
    csv_file = tmp_path / "write_check.csv"
    csv_file.write_text("test")
    sql_file = tmp_path / "write_check.sql"
    result = dc.write_check(sql, sql_file)
    assert not result
    assert not sql_file.exists()
    assert csv_file.exists()
    assert csv_file.read_text().strip() == "test"


def test_write_check_existing_csv_force(dc: DataCheck, tmp_path: Path):
    dc.config.force = True
    sql = "select 1 as a union all select 2 as a"
    sql_file = tmp_path / "write_check.sql"
    csv_file = tmp_path / "write_check.csv"
    csv_file.write_text("test")
    result = dc.write_check(sql, sql_file)
    assert result
    assert sql_file.exists()
    assert csv_file.exists()
    assert sql_file.read_text() == sql
    assert csv_file.read_text().strip() == "a\n1\n2"


def test_write_check_existing_sql(dc: DataCheck, tmp_path: Path):
    sql = "select 1 as a union all select 2 as a"
    csv_file = tmp_path / "write_check.csv"
    sql_file = tmp_path / "write_check.sql"
    sql_file.write_text("select")
    result = dc.write_check(sql, sql_file)
    assert not result
    assert sql_file.exists()
    assert not csv_file.exists()
    assert sql_file.read_text().strip() == "select"


def test_write_check_existing_sql_force(dc: DataCheck, tmp_path: Path):
    dc.config.force = True
    sql = "select 1 as a union all select 2 as a"
    sql_file = tmp_path / "write_check.sql"
    sql_file.write_text("select")
    csv_file = tmp_path / "write_check.csv"
    result = dc.write_check(sql, sql_file)
    assert result
    assert sql_file.exists()
    assert csv_file.exists()
    assert sql_file.read_text() == sql
    assert csv_file.read_text().strip() == "a\n1\n2"


def test_write_check_existing_csv_and_sql(dc: DataCheck, tmp_path: Path):
    sql = "select 1 as a union all select 2 as a"
    csv_file = tmp_path / "write_check.csv"
    csv_file.write_text("test")
    sql_file = tmp_path / "write_check.sql"
    sql_file.write_text("select")
    result = dc.write_check(sql, sql_file)
    assert not result
    assert sql_file.exists()
    assert csv_file.exists()
    assert csv_file.read_text().strip() == "test"
    assert sql_file.read_text().strip() == "select"


def test_write_check_existing_csv_and_sql_force(dc: DataCheck, tmp_path: Path):
    dc.config.force = True
    sql = "select 1 as a union all select 2 as a"
    csv_file = tmp_path / "write_check.csv"
    csv_file.write_text("test")
    sql_file = tmp_path / "write_check.sql"
    sql_file.write_text("select")
    result = dc.write_check(sql, sql_file)
    assert result
    assert sql_file.exists()
    assert csv_file.exists()
    assert sql_file.read_text() == sql
    assert csv_file.read_text().strip() == "a\n1\n2"

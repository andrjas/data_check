from pathlib import Path
from unittest.mock import create_autospec
import pandas as pd


from data_check.checks.generator import DataCheckGenerator  # noqa E402
from data_check.sql import DataCheckSql  # noqa E402
from data_check import DataCheck  # noqa E402


def prepare_sql(tmp_path: Path):
    sql_file = tmp_path / "a.sql"
    sql_text = "select 1 as test"
    sql_file.write_text(sql_text)
    sql = create_autospec(DataCheckSql, instance=True)
    sql.run_query.return_value = pd.DataFrame.from_dict({"test": [1]})
    expect_result = tmp_path / "a.csv"
    data_check = DataCheck()
    data_check.sql = sql
    return data_check, sql, sql_file, sql_text, expect_result


def test_gen_expectation(tmp_path: Path):
    data_check, sql, sql_file, sql_text, expect_result = prepare_sql(tmp_path=tmp_path)

    generator = DataCheckGenerator(data_check, sql_file)
    generator.gen_expectation(sql_file=sql_file)

    sql.run_query.assert_called_once_with(sql_text)

    assert expect_result.exists()
    assert expect_result.read_text().strip() == "test\n1"


def test_gen_expectation_no_overwrite(tmp_path: Path):
    data_check, sql, sql_file, _, expect_result = prepare_sql(tmp_path=tmp_path)
    expect_result.write_text("test")

    generator = DataCheckGenerator(data_check, sql_file)
    generator.gen_expectation(sql_file=sql_file)

    sql.run_query.assert_not_called()

    assert expect_result.exists()
    assert expect_result.read_text().strip() == "test"


def test_gen_expectation_force_overwrite(tmp_path: Path):
    data_check, sql, sql_file, sql_text, expect_result = prepare_sql(tmp_path=tmp_path)
    expect_result.write_text("test")

    generator = DataCheckGenerator(data_check, sql_file)
    generator.gen_expectation(sql_file=sql_file, force=True)

    sql.run_query.assert_called_once_with(sql_text)

    assert expect_result.exists()
    assert expect_result.read_text().strip() == "test\n1"

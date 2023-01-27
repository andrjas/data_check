from pathlib import Path
from unittest.mock import create_autospec

import pandas as pd

from data_check import DataCheck, DataCheckConfig  # noqa E402
from data_check.checks.csv_check import CSVCheck
from data_check.checks.generator import DataCheckGenerator  # noqa E402
from data_check.checks.table_check import TableCheck
from data_check.sql.query_result import QueryResult


def prepare_sql(tmp_path: Path):
    sql_file = tmp_path / "a.sql"
    sql_text = "select 1 as test"
    sql_file.write_text(sql_text)

    expect_result = tmp_path / "a.csv"
    config = DataCheckConfig()
    config.set_connection("")
    data_check = DataCheck(config=config)

    check = create_autospec(CSVCheck, instance=True)
    qr = QueryResult(sql_text, None)
    qr._df = pd.DataFrame.from_dict({"test": [1]})
    check.get_sql_result.return_value = qr
    return data_check, sql_file, expect_result, check


def prepare_with_table_check(tmp_path: Path):
    sql_file = tmp_path / "a.sql"
    sql_text = "select 1 as test"
    sql_file.write_text(sql_text)

    expect_result = tmp_path / "a.csv"
    config = DataCheckConfig()
    config.set_connection("")
    data_check = DataCheck(config=config)

    check = create_autospec(TableCheck, instance=True)
    qr = QueryResult(sql_text, None)
    qr._df = pd.DataFrame.from_dict({"test": [1]})
    check.get_sql_result.return_value = qr
    return data_check, sql_file, expect_result, check


def test_gen_expectation(tmp_path: Path):
    data_check, sql_file, expect_result, check = prepare_sql(tmp_path=tmp_path)

    generator = DataCheckGenerator(data_check, sql_file, check)
    generator.gen_expectation(sql_file=sql_file)

    check.get_sql_result.assert_called_once()

    assert expect_result.exists()
    assert expect_result.read_text().strip() == "test\n1"


def test_gen_expectation_no_overwrite(tmp_path: Path):
    data_check, sql_file, expect_result, check = prepare_sql(tmp_path=tmp_path)
    expect_result.write_text("test")

    generator = DataCheckGenerator(data_check, sql_file, check)
    generator.gen_expectation(sql_file=sql_file)

    check.get_sql_result.assert_not_called()

    assert expect_result.exists()
    assert expect_result.read_text().strip() == "test"


def test_gen_expectation_force_overwrite(tmp_path: Path):
    data_check, sql_file, expect_result, check = prepare_sql(tmp_path=tmp_path)
    expect_result.write_text("test")

    generator = DataCheckGenerator(data_check, sql_file, check)
    generator.gen_expectation(sql_file=sql_file, force=True)

    check.get_sql_result.assert_called_once()

    assert expect_result.exists()
    assert expect_result.read_text().strip() == "test\n1"


def test_gen_with_table_check(tmp_path: Path):
    data_check, sql_file, expect_result, check = prepare_with_table_check(
        tmp_path=tmp_path
    )

    generator = DataCheckGenerator(data_check, sql_file, check)
    generator.gen_expectation(sql_file=sql_file)

    check.get_sql_result.assert_called_once()

    assert expect_result.exists()
    assert expect_result.read_text().strip() == "test\n1"

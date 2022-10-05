from click.testing import CliRunner, Result
import pytest
from pathlib import Path
from contextlib import suppress

from typing import List, Optional


from data_check.cli.main import cli


def cleanup():
    for f in (
        "checks/pipelines/fake_data/main.simple_fake_table.csv",
        "checks/pipelines/fake_data/main.simple_fake_table_2.csv",
        "checks/generated/data_with_hash.csv",
        "test.db",
    ):
        with suppress(FileNotFoundError, PermissionError):
            Path(f).unlink()
        with suppress(FileNotFoundError, PermissionError):
            (Path("example") / f).unlink()


def prepare_sql():
    run(["sql", "--workers", "1", "--files", "prepare"])


@pytest.fixture(scope="session", autouse=True)
def prepare_int_tests():
    cleanup()
    prepare_sql()
    yield
    cleanup()


def run(command: List[str], workers: Optional[int] = 1) -> Result:
    runner = CliRunner()
    if not workers:
        workers_cmd = []
    else:
        workers_cmd = ["-n", str(workers)]
    result = runner.invoke(cli, command + workers_cmd)
    return result


def assert_passed(result: Result):
    assert result.exit_code == 0
    assert "overall result: PASSED" in result.output


def assert_failed(result: Result):
    assert result.exit_code == 1
    assert "overall result: FAILED" in result.output


def should_pass(command: List[str], workers: Optional[int] = 1):
    res = run(command=command, workers=workers)
    assert_passed(res)


@pytest.fixture(
    scope="module",
    params=[
        "checks/basic",
        "checks/empty_sets/basic",
        "checks/excel/basic",
        "checks/pipelines/simple_pipeline",
        "checks/pipelines/date_test",
        "checks/pipelines/leading_zeros",
        "checks/pipelines/table_check",
        "checks/pipelines/fake_data",
        "checks/pipelines/large_number",
    ],
)
def passing_tests(request):
    return request.param


def test_passing_tests(passing_tests):
    res = run(command=[passing_tests])
    assert_passed(res)


@pytest.fixture(
    scope="module",
    params=[
        "checks/failing/duplicates.sql",
        "checks/failing/expected_to_fail.sql",
        "checks/failing/invalid_csv.sql",
        "checks/failing/invalid.sql",
        "checks/empty_sets/failing/not_empty_query.sql",
        "checks/excel/failing/failing_empty.sql",
        "checks/excel/failing/failing_excel.sql",
    ],
)
def failing_tests(request):
    return request.param


def test_failing_tests(failing_tests):
    res = run(command=[failing_tests])
    assert_failed(res)


def test_generated():
    run(["gen", "checks/generated"])
    res = run(["checks/generated"])
    assert_passed(res)

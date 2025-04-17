from typing import Optional

import pytest
from click.testing import CliRunner, Result

from data_check.cli.main import cli


def run(command: list[str], workers: Optional[int] = 1) -> Result:
    # ignore unclosed SSLSocket ResourceWarning for Databricks
    import warnings

    warnings.filterwarnings(
        action="ignore", message="unclosed", category=ResourceWarning
    )

    runner = CliRunner()
    workers_cmd = [] if not workers else ["-n", str(workers)]
    result = runner.invoke(cli, command + workers_cmd)
    return result


def assert_passed(result: Result):
    assert result.exit_code == 0
    assert ": PASSED" in result.output


def assert_failed(result: Result):
    assert result.exit_code == 1
    assert ": FAILED" in result.output


def should_pass(command: list[str], workers: Optional[int] = 1):
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
    print(res.output)
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
    print(res.output)
    assert_failed(res)


def test_generated():
    run(["gen", "checks/generated"])
    res = run(["checks/generated"])
    assert_passed(res)

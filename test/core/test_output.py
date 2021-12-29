import pandas as pd
from pandas.testing import assert_frame_equal
from pathlib import Path
from typing import cast
import pytest

from data_check.output import DataCheckOutput  # noqa E402
from data_check.result import DataCheckResult


def test_pprint_failed_output_is_sorted():
    sorted_df = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    test_df = pd.DataFrame.from_dict({"id": [1, 0, 2], "data": ["b", "a", "c"]})
    out = DataCheckOutput()
    df = out.prepare_pprint_df(test_df)
    assert_frame_equal(df.reset_index(drop=True), sorted_df.reset_index(drop=True))


@pytest.fixture
def dc_out(tmp_path: Path):
    out = DataCheckOutput()
    log_file = tmp_path / "test.log"
    out.configure_output(
        verbose=False,
        traceback=False,
        print_failed=False,
        log_path=log_file,
        print_format=out.print_format,
    )
    return out


def test_print_writes_always_traceback_to_log(dc_out: DataCheckOutput):
    exc_test = "test exception"
    result = DataCheckResult(passed=False, source="test", exception=Exception(exc_test))
    dc_out.print(result)
    assert exc_test in cast(Path, dc_out.log_path).read_text()


def test_print_writes_always_failed_to_log(dc_out: DataCheckOutput):
    result = DataCheckResult(
        passed=False,
        source="test",
        result=pd.DataFrame.from_dict({"id": [1], "test_data": ["a"]}),
    )
    dc_out.print(result)
    assert "test_data" in cast(Path, dc_out.log_path).read_text()

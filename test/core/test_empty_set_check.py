from pathlib import Path
import pandas as pd
from pandas.testing import assert_frame_equal

from data_check import DataCheck


def test_empty_set_check(dc: DataCheck):
    check = dc.get_check(Path("checks/empty_sets/basic/empty_query.sql"))
    result = check.run_test()
    assert result


def test_empty_set_check_failing(dc: DataCheck):
    check = dc.get_check(Path("checks/empty_sets/failing/not_empty_query.sql"))
    result = check.run_test()
    assert not result


def test_empty_set_check_failing_returns_query_data(dc: DataCheck):
    check = dc.get_check(Path("checks/empty_sets/failing/not_empty_query.sql"))
    result = check.run_test()

    exp_df = pd.DataFrame.from_dict(
        {
            "a": [1],
            "_merge": ["left_only"],
        }
    )
    assert_frame_equal(exp_df, result.result)

import pandas as pd
from pandas.testing import assert_frame_equal


from data_check.output import DataCheckOutput  # noqa E402


def test_pprint_failed_output_is_sorted():
    sorted_df = pd.DataFrame.from_dict({"id": [0, 1, 2], "data": ["a", "b", "c"]})
    test_df = pd.DataFrame.from_dict({"id": [1, 0, 2], "data": ["b", "a", "c"]})
    out = DataCheckOutput()
    df = out.prepare_pprint_df(test_df)
    assert_frame_equal(df.reset_index(drop=True), sorted_df.reset_index(drop=True))

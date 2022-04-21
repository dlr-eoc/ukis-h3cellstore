import pandas as pd

from bamboo_h3.util import range_extender


def test_both_ends_missing():
    start = "2020-02-01 00:00:00"
    end = "2020-02-03 00:00:00"

    df = pd.DataFrame([42], columns=["answer"], index=[pd.Timestamp("2020-02-02 00:00:00", tz="UTC")])
    result = range_extender(df, start, end)
    assert len(result) == 3
    assert df["answer"].hasnans is False


def test_one_end_missing():
    start = "2020-02-01 00:00:00"
    end = "2020-02-02 00:00:00"

    df = pd.DataFrame([42], columns=["answer"], index=[pd.Timestamp(start, tz="UTC")])
    result = range_extender(df, start, end)
    assert len(result) == 2
    assert df["answer"].hasnans is False

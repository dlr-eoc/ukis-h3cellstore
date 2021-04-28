import numpy as np
import pytz
from bamboo_h3.columnset import _to_datetimeindex


def test_numpy_datetime64_to_pandas_datetimeindex(benchmark):
    timestamps = np.arange(100000, 1000000, dtype="datetime64[s]")
    result = benchmark(_to_datetimeindex, timestamps)

    assert len(result) == len(timestamps)
    assert result.tz == pytz.utc
    assert result[0].to_numpy() == timestamps[0]
from __future__ import annotations  # https://stackoverflow.com/a/33533514

from typing import Dict

import loguru
import numpy as np
import pandas as pd
import pytz

from loguru import logger
from . import bamboo_h3 as nativelib


class ColumnSet:
    inner: lib.ColumnSet

    def __init__(self, inner: lib.ColumnSet) -> ColumnSet:
        self.inner = inner

    @staticmethod
    def from_dataframe(df: pd.DataFrame, drain: bool = False) -> ColumnSet:
        """
        convert an pandas dataframe to an ColumnSet class

        >>> import pandas as pd
        >>> import numpy as np
        >>> df = pd.DataFrame({"x": np.zeros(12, dtype='uint8')})
        >>> column_set = ColumnSet.from_dataframe(df)
        >>> len(column_set)
        12
        """
        cs = nativelib.ColumnSet()
        for column_name in df.columns:
            col = df[column_name]
            if isinstance(col.dtype, pd.DatetimeTZDtype):
                # https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#from-timestamps-to-epoch
                timestamps = ((col.dt.tz_convert("UTC") - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta("1s",
                                                                                                                tz="UTC")).to_numpy()
                cs.add_numpy_datetime_column(column_name, timestamps)
            else:
                try:
                    cs.add_numpy_column(column_name, col.to_numpy())
                except Exception:
                    logger.error(f"Adding pandas column {column_name} typed {col.dtype} to columnset failed")
                    raise
            if drain:
                del df[column_name]
        return ColumnSet(cs)

    @property
    def column_types(self) -> Dict[str, str]:
        """

        >>> import pandas as pd
        >>> import numpy as np
        >>> df = pd.DataFrame({"x": np.zeros(12, dtype='uint8')})
        >>> column_set = ColumnSet.from_dataframe(df)
        >>> column_set.column_types['x']
        'u8'

        :return:
        """
        return self.inner.column_types

    def __len__(self) -> int:
        """
        >>> import pandas as pd
        >>> import numpy as np
        >>> df = pd.DataFrame({"x": np.zeros(12, dtype='uint8')})
        >>> column_set = ColumnSet.from_dataframe(df)
        >>> len(column_set)
        12

        :return: int
        """
        return len(self.inner)

    @property
    def size(self) -> int:
        """size property, equally to the one provided by pandas"""
        return len(self.inner)

    @property
    def empty(self) -> bool:
        return self.inner.empty

    def to_compacted(self, h3index_column_name: str = "h3index") -> ColumnSet:
        """
        Compact the h3indexes in the columnset and return a new columnset
        with the compacted data.

        Reduces the storage volume

        :param h3index_column_name:
        :return:
        """
        return ColumnSet(self.inner.to_compacted(h3index_column_name))

    def split_by_resolution(self, h3index_column_name: str, validate_indexes: bool = False) -> Dict[int, ColumnSet]:
        """
        split the columnset into parts depending on the h3 resolution used
        in the given h3index column

        :param h3index_column_name:
        :param validate_indexes:
        :return:
        """
        parts = self.inner.split_by_resolution(h3index_column_name, validate_indexes=validate_indexes)
        out_dict = {}
        for h3_res, cs_inner in parts.items():
            out_dict[h3_res] = ColumnSet(cs_inner)
        return out_dict

    def to_dataframe(self) -> pd.DataFrame:
        """
        drains the resultset into a pandas dataframe.

        draining means that the data gets moved to avoid duplication and increased
        memory requirements. The columnset will be empty afterwards

        >>> import pandas as pd
        >>> import numpy as np
        >>> df = pd.DataFrame({"x": np.zeros(12, dtype='uint8')})
        >>> column_set = ColumnSet.from_dataframe(df)
        >>> len(column_set)
        12
        >>> df2 = column_set.to_dataframe()
        >>> df.equals(df2)
        True
        >>> len(column_set)
        0

        """
        data = {}
        for column_name, column_type in self.column_types.items():
            array = None
            if column_type == 'u8':
                array = self.inner.drain_column_u8(column_name)
            elif column_type == 'i8':
                array = self.inner.drain_column_i8(column_name)
            elif column_type == 'u16':
                array = self.inner.drain_column_u16(column_name)
            elif column_type == 'i16':
                array = self.inner.drain_column_i16(column_name)
            elif column_type == 'u32':
                array = self.inner.drain_column_u32(column_name)
            elif column_type == 'i32':
                array = self.inner.drain_column_i32(column_name)
            elif column_type == 'u64':
                array = self.inner.drain_column_u64(column_name)
            elif column_type == 'i64':
                array = self.inner.drain_column_i64(column_name)
            elif column_type == 'f32':
                array = self.inner.drain_column_f32(column_name)
            elif column_type == 'f64':
                array = self.inner.drain_column_f64(column_name)
            elif column_type == 'date':
                array = _to_datetimeindex(
                    np.asarray(self.inner.drain_column_date(column_name), dtype='datetime64[s]'))
            elif column_type == 'datetime':
                array = _to_datetimeindex(
                    np.asarray(self.inner.drain_column_datetime(column_name), dtype='datetime64[s]'))
            else:
                raise NotImplementedError(f"unsupported column type: {column_type}")
            data[column_name] = array
        return pd.DataFrame(data)

    def __repr__(self) -> str:
        """
        Get a representation of the object.

        >>> import pandas as pd
        >>> import numpy as np
        >>> df = pd.DataFrame({"x": np.zeros(12, dtype='uint8'), "y": np.zeros(12, dtype='uint8')})
        >>> column_set = ColumnSet.from_dataframe(df)
        >>> repr(column_set)
        'ColumnSet(x, y)[12 rows]'

        :return: str
        """
        return repr(self.inner)


def _to_datetimeindex(timestamps: np.array) -> pd.DatetimeIndex:
    """
    directly create an datetimeindex from a numpy array.

    This impl omits some parsing overhead as the inputs are well defined. Normally
    you would use something like

    ```
    pd.to_datetime(timestamps, utc=True, infer_datetime_format=True)
    ```

    but that approx. doubles the runtime in benchmarks.

    :param timestamps: numpy array of UNIX timestamps (dtype='datetime[s]'), expected tz=UTC
    :return:
    """
    return pd.DatetimeIndex(timestamps, tz=pytz.utc, copy=False)


if __name__ == "__main__":
    # run doctests
    import doctest

    doctest.testmod(verbose=True)

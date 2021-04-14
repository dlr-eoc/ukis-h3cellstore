from __future__ import annotations  # https://stackoverflow.com/a/33533514

from typing import Dict

import numpy as np
import pandas as pd

from . import bamboo_h3 as lib


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
        cs = lib.ColumnSet()
        for column_name in df.columns:
            # TODO: convert numpy datetimes to datetime[s]
            # numpy uses uint64 for all datetimes, see https://docs.scipy.org/doc/numpy-1.13.0/reference/arrays.datetime.html#datetime-units
            cs.add_numpy_column(column_name, df[column_name].to_numpy())
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
    def size(self) -> bool:
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
                array = np.asarray(self.inner.drain_column_date(column_name), dtype='datetime64[s]')
            elif column_type == 'datetime':
                array = np.asarray(self.inner.drain_column_datetime(column_name),
                                   dtype='datetime64[s]')
            else:
                raise NotImplementedError(f"unsupported column type: {column_type}")
            data[column_name] = array
        return pd.DataFrame(data)

    def write_to(self, filename: str) -> None:
        """
        serialize to a file.

        Uses CBOR serialization with ZSTD compression
        """
        self.inner.write_to(filename)

    @staticmethod
    def read_from(filename: str) -> ColumnSet:
        """
        deserialize from a file.

        expects CBOR serialization with ZSTD compression

        >>> from tempfile import gettempdir
        >>> import pandas as pd
        >>> import numpy as np
        >>> df = pd.DataFrame({"x": np.random.rand(2000)})
        >>> column_set = ColumnSet.from_dataframe(df)
        >>> column_set.column_types
        {'x': 'f64'}
        >>> filename = f"{gettempdir()}/columnset.cbor.zstd"
        >>> column_set.write_to(filename)
        >>> column_set2 = ColumnSet.read_from(filename)
        >>> len(column_set2)
        2000
        >>> column_set2.column_types
        {'x': 'f64'}
        """
        inner = lib.ColumnSet.read_from(filename)
        return ColumnSet(inner)

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


if __name__ == "__main__":
    # run doctests
    import doctest

    doctest.testmod(verbose=True)

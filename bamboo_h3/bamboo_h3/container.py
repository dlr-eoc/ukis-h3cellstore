import pandas as pd
import numpy as np

from . import bamboo_h3 as lib


class ColumnSet:
    inner: lib.ColumnSet

    def __init__(self, inner: lib.ColumnSet):
        self.inner = inner

    @staticmethod
    def from_dataframe(df: pd.DataFrame, drain=False):
        """
        convert an pandas dataframe to an ColumnSet class

        >>> import pandas as pd
        >>> import numpy as np
        >>> df = pd.DataFrame({"x": np.zeros(12, dtype='uint8')})
        >>> column_set = ColumnSet.from_dataframe(df)
        >>> len(column_set)
        12
        """
        inner = lib.ColumnSet()
        for column_name in df.columns:
            inner.add_numpy_column(column_name, df[column_name].to_numpy())
            if drain:
                del df[column_name]
        return ColumnSet(inner)

    @property
    def column_types(self):
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
        return len(self.inner)

    @property
    def is_empty(self) -> bool:
        return self.inner.is_empty

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


if __name__ == "__main__":
    # run doctests
    import doctest

    doctest.testmod()

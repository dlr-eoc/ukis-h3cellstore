# noinspection PyUnresolvedReferences
from .ukis_h3cellstorepy import PyDataFrame, PyH3DataFrame
import pyarrow as pa
import typing

try:
    import polars as pl

    _HAS_POLARS = True
except ImportError:
    _HAS_POLARS = False

try:
    import pandas as pd

    _HAS_PANDAS = True
except ImportError:
    _HAS_PANDAS = False

DataFrameLike = typing.Union[PyDataFrame, PyH3DataFrame, pa.Table, "pl.DataFrame", "pd.DataFrame"]


class DataFrameWrapper:
    """implements most of the arrow/dataframe conversion fun"""

    def __init__(self, df: DataFrameLike):
        self._df = df

    def h3index_column_name(self) -> str:
        """name of the column the h3indexes are stored in"""
        if isinstance(self._df, PyDataFrame) or isinstance(self._df, PyH3DataFrame):
            return self._df.h3index_column_name()
        else:
            raise ValueError("contained type holds nt h3index_column_name meta information")

    def to_arrow(self) -> pa.Table:
        if isinstance(self._df, pa.Table):
            return self._df
        if isinstance(self._df, PyDataFrame) or isinstance(self._df, PyH3DataFrame):
            chunks = self._df.to_arrow()
            return pa.Table.from_batches(chunks)
        if _HAS_POLARS:
            if isinstance(self._df, pl.DataFrame):
                return self._df.to_arrow()
        if _HAS_PANDAS:
            if isinstance(self._df, pd.DataFrame):
                return pa.Table.from_pandas(self._df)
        raise TypeError("unsupported type")

    def to_polars(self) -> "pl.DataFrame":
        """Convert to a polars dataframe.

        In most cases this should be a zero-copy operation

        Requires having polars installed."""
        if not _HAS_POLARS:
            raise RuntimeError("polars is required")
        if isinstance(self._df, pl.DataFrame):
            return self._df
        if isinstance(self._df, pa.Table):
            return pl.from_arrow(self._df)
        if isinstance(self._df, PyDataFrame) or isinstance(self._df, PyH3DataFrame):
            return pl.from_arrow(self.to_arrow())
        if _HAS_PANDAS and isinstance(self._df, pd.DataFrame):
            return pl.from_pandas(self._df)
        raise TypeError("unsupported type")

    def to_pandas(self) -> "pd.DataFrame":
        """Convert to a pandas dataframe.

        Requires having pandas installed."""
        if not _HAS_PANDAS:
            raise RuntimeError("pandas is required")
        if isinstance(self._df, pd.DataFrame):
            return self._df
        if isinstance(self._df, pa.Table):
            return self._df.to_pandas()
        if isinstance(self._df, PyDataFrame) or isinstance(self._df, PyH3DataFrame):
            return self.to_arrow().to_pandas()
        if _HAS_POLARS and isinstance(self._df, pl.DataFrame):
            self._df.to_pandas()
        raise TypeError("unsupported type")


def ensure_wrapped(framelike: typing.Union[DataFrameWrapper, DataFrameLike]) -> DataFrameWrapper:
    """Create a `DataFrameWrapper` instance from the given input object"""
    if isinstance(framelike, DataFrameWrapper):
        return framelike
    return DataFrameWrapper(framelike)

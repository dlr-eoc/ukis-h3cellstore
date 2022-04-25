from .h3cellstorepy import PyDataFrame, PyH3DataFrame
import pyarrow as pa
import typing

try:
    import polars as pl

    _HAS_POLARS = True
except ImportError:
    _HAS_POLARS = False


class DataFrameWrapper:
    """implements most of the arrow/dataframe conversion fun"""

    def __init__(self, df: typing.Union[PyDataFrame, PyH3DataFrame, pa.Table, "pl.DataFrame"]):
        self._df = df

    def to_arrow(self) -> pa.Table:
        if isinstance(self._df, pa.Table):
            return self._df
        if isinstance(self._df, PyDataFrame) or isinstance(self._df, PyH3DataFrame):
            chunks = self._df.to_arrow()
            return pa.Table.from_batches(chunks)

        if _HAS_POLARS:
            if isinstance(self._df, pl.DataFrame):
                return self._df.to_arrow()
        raise TypeError("unsupported type")

    def to_polars(self) -> pl.DataFrame:
        if not _HAS_POLARS:
            raise RuntimeError("polars is required")
        if isinstance(self._df, pl.DataFrame):
            return self._df
        if isinstance(self._df, pa.Table):
            return pl.from_arrow(self._df)
        if isinstance(self._df, PyDataFrame) or isinstance(self._df, PyH3DataFrame):
            return pl.from_arrow(self.to_arrow())
        raise TypeError("unsupported type")

    # TODO: pandas

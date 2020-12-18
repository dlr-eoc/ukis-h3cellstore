
# import from rust library
from .h3cpy import CompactedTable, \
    ClickhouseConnection as _ClickhouseConnection, \
    version
from .geometry import to_geojson_string
import pandas as pd

__all__ = [
    "CompactedTable",
    "ClickhouseConnection",
    "poc_some_dataframe"
]

__version__ = version()

class ClickhouseConnection:
    inner = None

    def __init__(self, *a, **kw):
        self.inner = _ClickhouseConnection(*a, **kw)

    def window_iter(self, window_polygon):
        return self.inner.window_iter(to_geojson_string(window_polygon))

    def poc_some_h3indexes(self):
        return self.inner.poc_some_h3indexes()

# proof of concepts - to be removed later
def poc_some_dataframe():
    return pd.DataFrame({
        "h3index": ClickhouseConnection().poc_some_h3indexes()
    })
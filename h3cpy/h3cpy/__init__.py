
# import from rust library
from .h3cpy import CompactedTable, \
    ClickhouseConnection as _ClickhouseConnection, \
    create_connection, \
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

    def __init__(self, url: str):
        self.inner = create_connection(url)

    def window_iter(self, window_polygon):
        return self.inner.window_iter(to_geojson_string(window_polygon))

    def poc_some_h3indexes(self):
        return self.inner.poc_some_h3indexes()

    def list_tablesets(self):
        """list all tablesets in the database"""
        return self.inner.list_tablesets()

# proof of concepts - to be removed later
def poc_some_dataframe():
    return pd.DataFrame({
        "h3index": ClickhouseConnection().poc_some_h3indexes()
    })
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

    def window_iter(self, window_polygon, tableset, h3_resolution, window_max_size=16000):
        """
        iterate in a sliding window over a tableset

        :param window_polygon: polygon (geojson stirng, or something which is understood by the geojson module)
        :param tableset: reference to the tableset to fetch
        :param h3_resolution: H3 resolution to fetch the data at
        :param window_max_size: data for how many h3indexes should be fetched at once
        :return: generator
        """
        sliding_window = self.inner.make_sliding_window(
            to_geojson_string(window_polygon),
            tableset,
            h3_resolution,
            window_max_size
        )
        while True:
            window_data = self.inner.fetch_next_window(sliding_window, tableset)
            if window_data is None:
                break
            yield window_data

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

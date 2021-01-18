# import from rust library
import geojson
import numpy as np
import pandas as pd

from . import h3cpy as lib
from .h3cpy import create_connection, \
    Polygon, \
    h3indexes_convex_hull, \
    version

__all__ = [
    "CompactedTable",
    "ClickhouseConnection",
    "ClickhouseResultSet",
    "Polygon",
    "h3indexes_convex_hull",
]

__version__ = version()


def to_polygon(input):
    if type(input) == Polygon:
        return input
    if type(input) == str:
        return Polygon.from_geojson(input)
    # geojson should also take care of objects implementing __geo_interface__
    # geo interface specification: https://gist.github.com/sgillies/2217756
    return Polygon.from_geojson(geojson.dumps(input))


class ClickhouseResultSet:
    resultset = None

    def __init__(self, rs):
        self.resultset = rs

    @property
    def column_types(self):
        return self.resultset.column_types

    @property
    def num_h3indexes_queried(self):
        """get the number of h3indexes which where used in the query"""
        return self.resultset.num_h3indexes_queried

    @property
    def h3indexes_queried(self):
        """get the h3indexes which where used in the query"""
        return self.resultset.h3indexes_queried

    @property
    def window_index(self):
        """get the h3index of the window in case this resultset was fetched in a sliding window"""
        return self.resultset.window_index

    def to_dataframe(self):
        """
        drains the resultset into a pandas dataframe.

        draining meeans that the data gets moved to avoid duplication and increased
        memory requirements. The resultset will be empty afterwards
        """
        data = {}
        for column_name, column_type in self.column_types.items():
            array = None
            if column_type == 'u8':
                array = lib.resultset_drain_column_u8(self.resultset, column_name)
            elif column_type == 'i8':
                array = lib.resultset_drain_column_i8(self.resultset, column_name)
            elif column_type == 'u16':
                array = lib.resultset_drain_column_u16(self.resultset, column_name)
            elif column_type == 'i16':
                array = lib.resultset_drain_column_i16(self.resultset, column_name)
            elif column_type == 'u32':
                array = lib.resultset_drain_column_u32(self.resultset, column_name)
            elif column_type == 'i32':
                array = lib.resultset_drain_column_i32(self.resultset, column_name)
            elif column_type == 'u64':
                array = lib.resultset_drain_column_u64(self.resultset, column_name)
            elif column_type == 'i64':
                array = lib.resultset_drain_column_i64(self.resultset, column_name)
            elif column_type == 'f32':
                array = lib.resultset_drain_column_f32(self.resultset, column_name)
            elif column_type == 'f64':
                array = lib.resultset_drain_column_f64(self.resultset, column_name)
            elif column_type == 'date':
                array = np.asarray(lib.resultset_drain_column_date(self.resultset, column_name), dtype='datetime64[s]')
            elif column_type == 'datetime':
                array = np.asarray(lib.resultset_drain_column_datetime(self.resultset, column_name),
                                   dtype='datetime64[s]')
            else:
                raise NotImplementedError(f"unsupported column type: {column_type}")
            data[column_name] = array
        return pd.DataFrame(data)


class ClickhouseConnection:
    inner = None

    def __init__(self, url: str):
        self.inner = create_connection(url)

    def window_iter(self, window_polygon, tableset, h3_resolution, window_max_size=16000, querystring_template=None):
        """
        iterate in a sliding window over a tableset

        :param window_polygon: polygon (geojson stirng, or something which is understood by the geojson module)
        :param tableset: reference to the tableset to fetch
        :param h3_resolution: H3 resolution to fetch the data at
        :param window_max_size: data for how many h3indexes should be fetched at once
        :param querystring_template: Template for the query string. .... TODO: write docs
        :return: generator
        """
        sliding_window = self.inner.make_sliding_window(
            to_polygon(window_polygon),
            tableset,
            h3_resolution,
            window_max_size,
            querystring_template=querystring_template
        )
        while True:
            window_data = self.inner.fetch_next_window(sliding_window, tableset)
            if window_data is None:
                break
            if window_data.is_empty():
                continue
            yield ClickhouseResultSet(window_data)

    def list_tablesets(self):
        """list all tablesets in the database"""
        return self.inner.list_tablesets()

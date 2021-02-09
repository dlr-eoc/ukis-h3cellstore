"""
postgres integration

All parameters named `cur` in this module are expected to be pyscopg2 cursors.
"""

import h3.api.numpy_int as h3
import numpy as np
import pandas as pd

from . import Polygon


def as_bytes(in_val):
    if type(in_val) == bytes:
        return in_val
    if type(in_val) == memoryview:
        return in_val.tobytes()
    raise ValueError("unable to convert to bytes")


def fetch_using_intersecting_h3indexes(cur, h3indexes: np.array, wkb_column_name: str, query_str: str, *query_args):
    """
    execute a sql query and return the rows for all results intersecting with a h3index of the
     given numpy array

    TODO: move more of this to rust?

    :param cur:
    :param h3indexes: numpy-array of h3indexes
    :param wkb_column_name: the name of the column containing a polygon in WKB format
    :param query_str: the query string to execute
    :param query_args: arguments for the query string
    :return:
    """

    cur.execute(query_str, *query_args)

    # calculate the h3index coordinates
    h3indexes_coords = []  # h3index with (x, y) coordinate
    for h3index in np.nditer(h3indexes):
        (lat, lon) = h3.h3_to_geo(h3index)
        h3indexes_coords.append((h3index, lon, lat))  # this is not an accidental switch
    dataframes = []
    column_names = []
    wkb_column_idx = None
    while True:
        row = cur.fetchone()
        if row is None:
            break
        if not column_names:
            # find some column indexes
            for (column_idx, column) in enumerate(cur.description):
                column_name = column[0]
                column_names.append(column_name)
                if column_name == wkb_column_name:
                    wkb_column_idx = column_idx
            if wkb_column_idx is None:
                raise IndexError("wkb column not found in query results")

        # read the wkb into a geometry instance, if this fails
        # the contents of the column a most certainly postgis EWKB instead of WKB
        poly = Polygon.from_wkb(as_bytes(row[wkb_column_idx]))

        # collect the h3indexes contained in the geometry of the row
        h3index_column = np.fromiter([h3index for (h3index, x, y) in h3indexes_coords if poly.contains_point(x, y)], np.uint64)

        if h3index_column.size > 0:
            resultdict = {}
            for (column_idx, value) in enumerate(row):
                if column_idx == wkb_column_idx:
                    continue
                resultdict[column_names[column_idx]] = [value, ]
            df = pd.DataFrame(resultdict).merge(
                pd.DataFrame({"h3index": h3index_column}),
                how='cross' # pandas >=1.2
            )
            dataframes.append(df)
    if dataframes:
        return pd.concat(dataframes)
    return pd.DataFrame({})

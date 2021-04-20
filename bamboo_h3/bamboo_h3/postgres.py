"""
postgres integration

All parameters named `cur` in this module are expected to be pyscopg2 cursors.
"""

import numpy as np
import pandas as pd

from . import ColumnSet
from .bamboo_h3 import intersect_columnset_with_indexes


def as_bytes(in_val) -> bytes:
    if type(in_val) == bytes:
        return in_val
    if type(in_val) == memoryview:
        return in_val.tobytes()
    raise ValueError("unable to convert to bytes")


def __wkb_and_df_from_query(cur, wkb_column_name: str, query_str: str, *query_args):
    cur.execute(query_str, *query_args)

    # assemble a dataframe and a list of wkbs from the query results
    df_columns = {}
    wkb_list = []
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
                else:
                    df_columns[column_name] = []
            if wkb_column_idx is None:
                raise IndexError(f"wkb column {wkb_column_name} not found in query results")
        for (column_idx, value) in enumerate(row):
            if column_idx == wkb_column_idx:
                wkb_list.append(as_bytes(value))
            else:
                df_columns[column_names[column_idx]].append(value)
    return pd.DataFrame(df_columns), wkb_list


def fetch_using_intersecting_h3indexes(cur, h3indexes: np.array, wkb_column_name: str, query_str: str,
                                       *query_args) -> pd.DataFrame:
    """
    execute a sql query and return the rows for all results intersecting with a h3index of the
     given numpy array

    :param cur:
    :param h3indexes: numpy-array of h3indexes
    :param wkb_column_name: the name of the column containing a polygon in WKB format
    :param query_str: the query string to execute
    :param query_args: arguments for the query string
    :return:
    """

    df, wkb_list = __wkb_and_df_from_query(cur, wkb_column_name, query_str, *query_args)
    columnset = ColumnSet.from_dataframe(df, drain=True)
    return ColumnSet(intersect_columnset_with_indexes(columnset.inner, wkb_list, h3indexes)).to_dataframe()

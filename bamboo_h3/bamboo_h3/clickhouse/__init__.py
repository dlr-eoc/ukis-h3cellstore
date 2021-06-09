from __future__ import annotations  # https://stackoverflow.com/a/33533514

from typing import Union, Optional, Generator, Dict

import numpy as np
import pandas as pd

from .schema import Schema
from ..columnset import ColumnSet
from ..geo import to_polygon, Polygon
from .. import BambooH3Error
from ..bamboo_h3 import CompactedTable, \
    TableSet, \
    ResultSet, \
    create_connection

__all__ = [
    "ClickhouseConnection",
    "ClickhouseResultSet",
    "AlreadyMoved",
    "TableSetNotFound",
    "TableSetLike",

    # accessing the imported function and classes to let IDEs know these are not
    # unused imports. They are only re-exported, but not used in this file.
    CompactedTable.__name__,
    TableSet.__name__,
    ResultSet.__name__,
]

TableSetLike = Union[TableSet, str]


class TableSetNotFound(BambooH3Error):
    tableset_name: str

    def __init__(self, tableset_name):
        self.tablset_name = tableset_name
        super().__init__(f"Tableset {tableset_name} not found")


class AlreadyMoved(BambooH3Error):
    """
    Data loaded can only be transformed once into a columnset or dataframe once.

    repeated attempts to do tha t will result in this exception
    """
    pass


class ClickhouseResultSet:
    resultset = None

    def __init__(self, rs: ResultSet):
        self.resultset = rs

    @property
    def query_duration_secs(self) -> Optional[float]:
        """
        The number of seconds the query took to execute.

        Only measured for async queries, so this may be None.
        Calling this results in waiting until the results are available.

        :return: float|None
        """
        return self.resultset.query_duration_secs

    @property
    def num_h3indexes_queried(self) -> int:
        """get the number of h3indexes which where used in the query"""
        return self.resultset.num_h3indexes_queried

    @property
    def h3indexes_queried(self) -> np.array:
        """get the h3indexes which where used in the query"""
        return self.resultset.h3indexes_queried

    @property
    def containing_index(self) -> Optional[int]:
        """get the h3index of the cell containing the data.

        This in field is populated in case this resultset was fetched during `walk`. Then in will contain
        the h3index of the current walk cell."""
        return self.resultset.containing_index

    def to_columnset(self) -> ColumnSet:
        """
        drains the resultset into a columnset.

        draining means that the data gets moved to avoid duplication and increased
        memory requirements. The resultset will be empty afterwards

        This method will wait for asynchronous queries to be finished executing.
        """
        inner_cs = self.resultset.to_columnset()
        if inner_cs is None:
            raise AlreadyMoved()
        return ColumnSet(inner_cs)

    def to_dataframe(self) -> pd.DataFrame:
        """
        drains the resultset into a pandas dataframe.

        draining means that the data gets moved to avoid duplication and increased
        memory requirements. The resultset will be empty afterwards

        This method will wait for asynchronous queries to be finished executing.
        """
        cs = self.to_columnset()
        return cs.to_dataframe()

    @property
    def empty(self) -> bool:
        """
        Calling this results in waiting until the results are available.
        """
        return self.resultset.empty


class ClickhouseConnection:
    """
    Connection to the clickhouse DB

    Query terminology
    =================

    In this documentation there are multiple kinds of SQL-queries with slight differences. Hopefully this
    section explains these differences a bit.

    Querystring
    -----------

    This as simple as it gets, it is just a SQL string without any placeholders or support for parameters.

    QueryTemplate
    -------------

    This is also just a string, also without any parameter support. The difference is that it is geared to be
    used with tableset. It can be applied to any resolution of a tableset and this library will use this query
    to dynamically query the compacted, lower resolutions of the tableset to be able to perform the uncompacting
    of data.

    The selected columns must include the h3indexes in a column named `h3index`

    The query must include these placeholders:
    * '<[table]>': will be filled with the table to be queried
    * '<[h3indexes]>': will be filled with an array of h3indexes used for the query

    Asynchronicity
    ==============

    Some query functionalities of this library are asynchronous. This is not to be confused with pythons `async`/`await`
    syntax. In contrast to the native python async support, this here does not require any special calling conventions,
    or an `asyncio` loop to execute.

    This library internally uses multiple threads. Async-Queries are send to background threads to execute. After
    that python can continue to do other work. Only when the data of the resultset is accessed, the python-thread
    will wait until the results of the query have arrived. In the optimal case, the query has already finished executing
    and python can directly access the data.

    Functions with are asynchronous are marked in their docstring.

    .. code-block:: python

        some_data = conn.query_fetch("select .....")

        # do something else

        df = some_data.to_dataframe() # now the query will be waited for

    """
    inner = None

    def __init__(self, url: str):
        self.inner = create_connection(url)

    def walk(self, area_polygon: Polygon, tableset: TableSet, r_target: int, fetch_max_num: int = 16000,
             querystring_template: str = None,
             prefetch_querystring_template: str = None) -> Generator[ClickhouseResultSet, None, None]:
        """
        walk/iterate through the data of a tableset.

        :param area_polygon: polygon (geojson string, or something which is understood by the geojson module)
        :param tableset: reference to the tableset to fetch
        :param r_target: H3 resolution to fetch the data at
        :param fetch_max_num: data for how many cells should be fetched at once
        :param querystring_template: QueryTemplate for the query string to fetch the data. Using this
                allows to use SQL JOINs, subqueries and SQL functions before getting the data in a
                dataframe.
                When not set the SELECT uses the columns of the tableset.
        :param prefetch_querystring_template: QueryTemplate for the prefetch. The prefetch query is used to determinate
                if it is worth to fetch the contents of a cell or not. It is issued against the table
                containing a coarser resolution (`r_walk`) so it needs to inspect far less data and should be faster.
                Additionally, the data is not read; the query must contain a column named `h3index`.
                When not set the same value as the `querystring_template` will be used with a `limit 1` appended
        :return: generator
        """
        cell_walker = self.inner.create_cell_walker(
            to_polygon(area_polygon),
            tableset,
            r_target,
            fetch_max_num,
            querystring_template=querystring_template,
            prefetch_querystring_template=prefetch_querystring_template,
        )
        try:
            while True:
                cell_data = cell_walker.fetch_next_cell()
                if cell_data is None:
                    break  # reached end of iteration
                if cell_data.empty:
                    continue  # skip empty cells
                yield ClickhouseResultSet(cell_data)
        finally:
            # close may raise exceptions occurred during fetching
            cell_walker.close()

    def __get_tableset(self, tableset: TableSetLike) -> TableSet:
        if isinstance(tableset, str):
            try:
                tableset = self.list_tablesets()[tableset]
            except KeyError:
                raise TableSetNotFound(tableset)
        return tableset

    def list_tablesets(self) -> Dict[str, TableSet]:
        """list all tablesets in the database"""
        return self.inner.list_tablesets()

    def drop_tableset(self, tableset: TableSetLike):
        """
        Drop a tableset from the database.

        This may fail when Clickhouse enforces its maximum allowed drop
        limit (default is 50GB)
        """
        return self.inner.drop_tableset(self.__get_tableset(tableset))

    def query_fetch(self, query_string: str) -> ClickhouseResultSet:
        """
        execute a query string.

        :return an asynchronous resultset
        """
        return ClickhouseResultSet(self.inner.query_fetch(query_string))

    def tableset_fetch(self, tableset: TableSetLike, h3indexes: np.array,
                       query_template: str = None) -> ClickhouseResultSet:
        """
        Fetch data for a given numpy-array of h3 indexes from a tableset. The query will be autogenerated to fetch
        all columns unless a query template is given via the 'query_template' parameter.

        Uncompacting is done automatically

        :return an asynchronous resultset
        """
        return ClickhouseResultSet(
            self.inner.tableset_fetch(
                self.__get_tableset(tableset), h3indexes, query_template=query_template)
        )

    def tableset_contains_h3index(self, tableset: TableSetLike, h3index: int,
                                  query_template: str = None) -> bool:
        """
        check if the tableset contains the h3index or any of its parents

        :return: bool
        """
        return self.inner.tableset_contains_h3index(self.__get_tableset(tableset), h3index,
                                                    query_template=query_template)

    def create_schema(self, schema: Schema):
        return self.inner.create_schema(schema)

    def save_columnset(self, schema: Schema, columnset: ColumnSet):
        return self.inner.save_columnset(schema, columnset.inner)

    def save_dataframe(self, schema: Schema, dataframe: pd.DataFrame, drain: bool = False):
        return self.save_columnset(schema, ColumnSet.from_dataframe(dataframe, drain=drain))

# noinspection PyUnresolvedReferences
from ..fixtures import clickhouse_grpc_endpoint, has_polars, has_pandas

from h3cellstorepy.clickhouse import GRPCConnection

import pytest


def test_connection(clickhouse_grpc_endpoint):
    GRPCConnection(clickhouse_grpc_endpoint, "default")


def test_connection_non_existing_db(clickhouse_grpc_endpoint):
    with pytest.raises(IOError):
        GRPCConnection(clickhouse_grpc_endpoint, "non-existing-db")


def test_connection_execute_error_propagation(clickhouse_grpc_endpoint):
    con = GRPCConnection(clickhouse_grpc_endpoint, "default")
    with pytest.raises(IOError) as excinfo:
        con.execute_into_dataframe("select something_invalid")
    assert "'something_invalid'" in str(excinfo)


def test_connection_execute_into_dataframe_polars(clickhouse_grpc_endpoint, has_polars):
    con = GRPCConnection(clickhouse_grpc_endpoint, "system")
    df = con.execute_into_dataframe("select name from databases").to_polars()
    import polars as pl
    assert isinstance(df, pl.DataFrame)
    assert df.shape[1] == 1


def test_connection_execute_into_dataframe_pandas(clickhouse_grpc_endpoint, has_pandas):
    con = GRPCConnection(clickhouse_grpc_endpoint, "system")
    df = con.execute_into_dataframe("select name from databases").to_pandas()
    import pandas as pd
    assert isinstance(df, pd.DataFrame)
    assert df.shape[1] == 1


def test_connection_execute_into_h3dataframe_polars(clickhouse_grpc_endpoint, has_polars):
    con = GRPCConnection(clickhouse_grpc_endpoint, "system")
    df_w = con.execute_into_h3dataframe("""
        select 
            arrayJoin(h3ToChildren(geoToH3(12.0, 20.0, 5), 8)) as my_h3index, 
            'something' as name
        """, "my_h3index")
    assert df_w.h3index_column_name() == "my_h3index"
    df = df_w.to_polars()
    import polars as pl
    assert isinstance(df, pl.DataFrame)
    assert df.shape == (pow(7, 3), 2)


def test_connection_database_exists(clickhouse_grpc_endpoint):
    con = GRPCConnection(clickhouse_grpc_endpoint, "system")
    assert con.database_exists("default")
    assert not con.database_exists("does_not_exist")

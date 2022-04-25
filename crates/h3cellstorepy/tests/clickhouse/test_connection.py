from ..fixtures import clickhouse_grpc_endpoint, has_polars
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


def test_connection_execute_into_dataframe(clickhouse_grpc_endpoint, has_polars):
    con = GRPCConnection(clickhouse_grpc_endpoint, "system")
    df = con.execute_into_dataframe("select name from databases").to_polars()
    assert df.shape[1] == 1

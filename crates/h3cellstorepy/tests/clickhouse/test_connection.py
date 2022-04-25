from ..fixtures import clickhouse_grpc_endpoint
from h3cellstorepy.clickhouse import connect_grpc

import pytest


def test_connection(clickhouse_grpc_endpoint):
    connect_grpc(clickhouse_grpc_endpoint, "default")


def test_connection_non_existing_db(clickhouse_grpc_endpoint):
    with pytest.raises(IOError):
        connect_grpc(clickhouse_grpc_endpoint, "non-existing-db")

import pytest
import os


def __clickhouse_grpc_endpoint():
    endpoint = os.environ.get("CLICKHOUSE_GRPC_TESTING_ENDPOINT")
    if not endpoint:
        raise pytest.skip()
    return endpoint


@pytest.fixture
def clickhouse_grpc_endpoint():
    return __clickhouse_grpc_endpoint()


@pytest.fixture
def pl():
    try:
        import polars
        return polars
    except ImportError:
        raise pytest.skip()


@pytest.fixture
def pd():
    try:
        import pandas
        return pandas
    except ImportError:
        raise pytest.skip()


@pytest.fixture
def clickhouse_testdb_name():
    return "test"

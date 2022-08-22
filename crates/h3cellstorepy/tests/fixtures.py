import pytest
import os
from pathlib import Path


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
def geojson():
    try:
        import geojson
        return geojson
    except ImportError:
        raise pytest.skip()


@pytest.fixture
def clickhouse_testdb_name():
    return "test"


@pytest.fixture
def rasterio():
    try:
        import rasterio
        return rasterio
    except ImportError:
        raise pytest.skip()


@pytest.fixture()
def testdata_path():
    return Path(__file__).parent.parent / "testdata"

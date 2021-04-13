import os

import geopandas as gpd
import pytest
from bamboo_h3 import ClickhouseConnection
from h3ronpy.util import h3index_column_to_geodataframe
from h3ronpy.vector import geodataframe_to_h3


def __clickhouse_dsn():
    clickhouse_dsn = os.environ.get("BAMBOO_CLICKHOUSE_DSN_TEST")
    if not clickhouse_dsn:
        raise pytest.skip()
    return clickhouse_dsn


@pytest.fixture
def clickhouse_dsn():
    return __clickhouse_dsn()


@pytest.fixture
def clickhouse_db():
    return ClickhouseConnection(__clickhouse_dsn())


def __naturalearth_africa_dataframe(h3_res=4):
    """
    an extract of the naturalearth dataset converted to
    :param h3_res:
    :return:
    """
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    africa = world[world["continent"] == "Africa"]
    return geodataframe_to_h3(africa, h3_res)


@pytest.fixture
def naturalearth_africa_dataframe():
    return __naturalearth_africa_dataframe()


@pytest.fixture
def naturalearth_africa_geodataframe():
    return h3index_column_to_geodataframe(__naturalearth_africa_dataframe())

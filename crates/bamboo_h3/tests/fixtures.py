import os

import geopandas as gpd
import numpy as np
import pytest
import rasterio
from bamboo_h3.clickhouse import ClickhouseConnection
from bamboo_h3.raster import raster_to_dataframe
from bamboo_h3.util import h3index_column_to_geodataframe
from bamboo_h3.vector import geodataframe_to_h3

from . import TESTDATA_PATH


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


def __naturalearth_africa_geodataframe():
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    world["country_id"] = np.array(range(1, len(world.index) + 1), dtype=np.uint16)
    africa = world[world["continent"] == "Africa"]
    return africa


@pytest.fixture
def naturalearth_africa_geodataframe():
    return __naturalearth_africa_geodataframe()


def __naturalearth_africa_dataframe(h3_res=4):
    """
    an extract of the naturalearth dataset converted to
    :param h3_res:
    :return:
    """
    return geodataframe_to_h3(__naturalearth_africa_geodataframe(), h3_res)


@pytest.fixture
def naturalearth_africa_dataframe_4():
    return __naturalearth_africa_dataframe(h3_res=4)


@pytest.fixture
def naturalearth_africa_dataframe_6():
    return __naturalearth_africa_dataframe(h3_res=6)


@pytest.fixture
def naturalearth_africa_dataframe_8():
    return __naturalearth_africa_dataframe(h3_res=8)


@pytest.fixture
def naturalearth_africa_geodataframe_4():
    return h3index_column_to_geodataframe(__naturalearth_africa_dataframe(h3_res=4))


def __r_tiff_dataframe(h3_res=4, compacted=True):
    dataset = rasterio.open(TESTDATA_PATH / "r.tiff")
    band = dataset.read(1)
    return raster_to_dataframe(band, dataset.transform, h3_res, nodata_value=0, compacted=compacted, geo=False)


@pytest.fixture
def r_tiff_dataframe_uncompacted_8():
    return __r_tiff_dataframe(h3_res=8, compacted=False)


@pytest.fixture
def r_tiff_dataframe_compacted_8():
    return __r_tiff_dataframe(h3_res=8, compacted=True)

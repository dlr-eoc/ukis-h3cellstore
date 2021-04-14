import json

import pytest
from bamboo_h3.concurrent import chunk_polygon
from shapely.geometry import shape, Polygon

from . import TESTDATA_PATH


def test_chunk_polygon():
    geom = shape(json.loads(open(TESTDATA_PATH / "fork-polygon.geometry.geojson").read()))
    chunked = chunk_polygon(geom, num_chunks_approx=20)
    assert 15 < len(chunked)
    assert len(chunked) < 30

    area_chunks = 0.0
    for chunk in chunked:
        assert isinstance(chunk, Polygon)
        area_chunks += chunk.area
    assert area_chunks == pytest.approx(geom.area, 0.000001)

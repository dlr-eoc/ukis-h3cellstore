import json

import pytest
from bamboo_h3.concurrent import chunk_polygon
from shapely.geometry import shape, Polygon

from . import TESTDATA_PATH


def test_chunk_polygon_complex():
    geom = shape(json.loads(open(TESTDATA_PATH / "fork-polygon.geometry.geojson").read()))
    chunked = chunk_polygon(geom, num_chunks_approx=20)
    assert 15 < len(chunked)
    assert len(chunked) < 30

    area_chunks = 0.0
    for chunk in chunked:
        assert isinstance(chunk, Polygon)
        area_chunks += chunk.area
    assert area_chunks == pytest.approx(geom.area, 0.000001)


def test_chunk_polygon():
    geom = shape(
        json.loads(
            """
            {
                "type": "Polygon",
                "coordinates": [
                    [
                        [35.474547, -17.284672],
                        [34.902956, -17.840163],
                        [36.079114, -18.85231],
                        [36.584752, -18.393927],
                        [35.474547, -17.284672]
                    ]
                ]
            }
            """
        )
    )
    chunked = chunk_polygon(geom, num_chunks_approx=16)
    assert 12 < len(chunked)
    assert len(chunked) < 20

import geojson
from h3cellstorepy.clickhouse import TraversalStrategy
import h3.api.numpy_int as h3


def test_create_geometry_traversal_instance():
    geom = geojson.loads("""
  {
        "type": "Polygon",
        "coordinates": [
          [
            [
              6.6796875,
              45.336701909968134
            ],
            [
              2.109375,
              46.558860303117164
            ],
            [
              -0.703125,
              44.84029065139799
            ],
            [
              1.40625,
              42.5530802889558
            ],
            [
              6.6796875,
              43.068887774169625
            ],
            [
              6.6796875,
              45.336701909968134
            ]
          ]
        ]
      }  
    """)
    strategy = TraversalStrategy.from_geometry(geom, 5)
    assert strategy.name() == "Geometry"
    assert strategy.h3_resolution() == 5


def test_create_cells_traversal_instance():
    cells = h3.k_ring(h3.geo_to_h3(20.0, 10.0, 5), 10)
    strategy = TraversalStrategy.from_cells(cells, 5)
    assert strategy.name() == "Cells"
    assert strategy.h3_resolution() == 5

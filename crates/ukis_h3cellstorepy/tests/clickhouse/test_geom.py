import geojson
from ukis_h3cellstorepy.geom import border_cells


def test_border_cells_works():
    # just a basic test if the integration works, more detailed tests are in the rust codebase
    center_point = (20.0, 20.0)
    coord_diff = 10.0
    geom = geojson.loads(f"""{{
            "type": "Polygon",
            "coordinates": [
              [
                [{center_point[0] - coord_diff}, {center_point[1] - coord_diff}],
                [{center_point[0] + coord_diff}, {center_point[1] - coord_diff}],
                [{center_point[0] + coord_diff}, {center_point[1] + coord_diff}],
                [{center_point[0] - coord_diff}, {center_point[1] + coord_diff}],
                [{center_point[0] - coord_diff}, {center_point[1] - coord_diff}]
              ]
            ]
          }}""")
    cells = border_cells(geom, 7)
    assert len(cells) > 100

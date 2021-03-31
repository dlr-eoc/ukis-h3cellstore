from shapely import wkb
from shapely.geometry import shape, Polygon as ShapelyPolygon
from typing import Any

from .bamboo_h3 import Polygon


def to_polygon(input: Any) -> Polygon:
    """
    convert ... something ... to a polygon.

    Using __geo_interface__ (https://gist.github.com/sgillies/2217756):

    >>> import geojson
    >>> geom = geojson.loads('{ "type": "Polygon", "coordinates": [ [ [ 15.1, 49.3 ], [ 18.6, 49.3 ], [ 18.6, 51.1 ], [ 15.1, 51.17 ], [ 15.1, 49.3 ] ] ] }')
    >>> p1 = to_polygon(geom)
    >>> p1.contains_point(17.3, 50.0)
    True

    :param input:
    :return:
    """
    if type(input) == Polygon:
        return input
    if type(input) == str:
        return Polygon.from_geojson(input)
    if type(input) == bytes:
        return Polygon.from_wkb(input)
    if isinstance(input, ShapelyPolygon):
        return Polygon.from_wkb(wkb.dumps(input))
    # shapely should also take care of objects implementing __geo_interface__
    # geo interface specification: https://gist.github.com/sgillies/2217756
    if hasattr(input, "__geo_interface__"):
        return Polygon.from_wkb(wkb.dumps(shape(input)))
    raise ValueError("unsupported type to convert to a geometry")


if __name__ == "__main__":
    # run doctests
    import doctest

    doctest.testmod(verbose=True)

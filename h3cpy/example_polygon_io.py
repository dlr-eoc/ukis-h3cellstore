import h3cpy
import h3.api.numpy_int as h3

# create a few indexes
indexes = h3.k_ring(h3.geo_to_h3(12.2, 34.4, 10), 3)
print(indexes)

# build a convex hull arrund them
hull = h3cpy.h3indexes_convex_hull(indexes)
print(hull.to_geojson_str())
print(hull.to_wkb())

# create a polygon from WKB
hull2 = h3cpy.Polygon.from_wkb(hull.to_wkb())
print(hull2.to_wkb())

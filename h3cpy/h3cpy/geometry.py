import geojson


def to_geojson_string(input):
    if type(input) == str:
        return input
    # geojson should also take care of objects implementing __geo_interface__
    # geo interface specification: https://gist.github.com/sgillies/2217756
    return geojson.dumps(input)

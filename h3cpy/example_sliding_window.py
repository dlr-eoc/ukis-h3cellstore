import h3cpy
import psycopg2
from h3cpy.postgres import fetch_using_intersecting_h3indexes

from datetime import datetime

# connect to postgres for metadata
# credentials see password db, here they are passed via PGUSER and PGPASSWORD environment variables (in libpq)
postgres_conn = psycopg2.connect("dbname=water2 host=127.0.0.1 port=5433")
postgres_cur = postgres_conn.cursor()

# connect to clickhouse
clickhouse_conn = h3cpy.ClickhouseConnection("tcp://localhost:9010/water2?compression=lz4")
tablesets = clickhouse_conn.list_tablesets()

# print all tablesets found
for tsname, ts in tablesets.items():
    print(f"tableset {tsname}")
    print(ts.base_resolutions)
    print(ts.compacted_resolutions)
    print(ts.columns)


# polygon geometry to visit
geom = """
{
        "type": "Polygon",
        "coordinates": [
          [
            [
              10.8984375,
              46.558860303117164
            ],
            [
              15.1171875,
              46.558860303117164
            ],
            [
              15.1171875,
              48.922499263758255
            ],
            [
              10.8984375,
              48.922499263758255
            ],
            [
              10.8984375,
              46.558860303117164
            ]
          ]
        ]
      }
"""

# iteratively visit all indexes using a h3-based sliding window
for resultset in clickhouse_conn.window_iter(geom, tablesets["water"], 12, window_max_size=6000):

    # the h3 index of the window itself. will have a lower resolution then the h3_resolution
    # requested for the window
    #print(resultset.window_index)

    # the h3indexes as used for the query
    #print(resultset.h3indexes_queried)

    # get as a pandas dataframe. This will move the data, so the resultset will be empty afterwards
    df = resultset.to_dataframe()
    print(df)

    if df.empty: # should not happen
        continue

    recording_timestamps = [datetime.utcfromtimestamp(ts.astype('O')/1e9) for ts in df.recorded_at.unique()]

    # to get missing values when there have been no detections, we must generate all timestamps when a index
    # has been covered by a scene - they are not stored. We just use the scene footprints to generate our subset of
    # h3indexes for each scene covering a h3index
    query_polygon = h3cpy.h3indexes_convex_hull(resultset.h3indexes_queried)
    print(query_polygon.to_geojson_str())
    scene_h3indexes_df = fetch_using_intersecting_h3indexes(
        postgres_cur,
        df.h3index.unique(), # just query for the h3index where we got data from clickhouse for. thats all we need to find holes in the timeseries
        "wkb_geom",
        """
        select s.id as scene_id, 
            s.sensor_id,
            s.processor_id,
            s.recorded_at,
            s.processed_at,
            st_asbinary(st_force2d(s.footprint)) wkb_geom 
        from scene s 
        where st_intersects(s.footprint, st_geomfromwkb(%s, 4326))
            and s.recorded_at = any(%s)
        """,
        (query_polygon.to_wkb(), recording_timestamps)
    )
    print(scene_h3indexes_df)
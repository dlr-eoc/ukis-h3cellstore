"""
This example is a possible blueprint for a simple processor crawling through the data
using multiple sliding windows in multiple processes
"""

import concurrent.futures
import json
import pandas as pd
import psycopg2
from datetime import datetime
from shapely.geometry import shape, Polygon

import h3cpy
from h3cpy.concurrent import chunk_polygon
from h3cpy.postgres import fetch_using_intersecting_h3indexes

# number of worker processes to use
MAX_WORKERS = 4

# postgres credentials see password db, here they are passed via PGUSER
# and PGPASSWORD environment variables (in libpq)
DSN_POSTGRES = "dbname=water2 host=127.0.0.1 port=5433"
DSN_CLICKHOUSE = "tcp://localhost:9010/water2?compression=lz4"

# polygon geometry to visit
AOI = """
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


def process_window(window_geom: Polygon):
    # connect to postgres for metadata
    postgres_conn = psycopg2.connect(DSN_POSTGRES)
    postgres_cur = postgres_conn.cursor()

    # connect to clickhouse
    clickhouse_conn = h3cpy.ClickhouseConnection(DSN_CLICKHOUSE)
    tablesets = clickhouse_conn.list_tablesets()

    # print all tablesets found
    for tsname, ts in tablesets.items():
        print(f"tableset {tsname} found")
        # print(ts.base_resolutions)
        # print(ts.compacted_resolutions)
        # print(ts.columns)

    querystring_template = """
    select h3index, 
        recorded_at,
        processed_at,
        area_percent_water_class_090_100, 
        area_percent_water_class_080_090, 
        area_percent_water_class_070_080, 
        sensor as sensor_id, 
        processor as processor_id
    from <[table]> 
    where recorded_at >= '2020-10-01 00:00:00' 
        and recorded_at < '2021-01-01 00:00:00'
        and (
            area_percent_water_class_090_100 > 0
            or area_percent_water_class_080_090 > 0
            or area_percent_water_class_070_080 > 0
        )
        and h3index in <[h3indexes]>
    """
    # iteratively visit all indexes using a h3-based sliding window
    for resultset in clickhouse_conn.window_iter(window_geom, tablesets["water"], 13, window_max_size=200000,
                                                 querystring_template=querystring_template):

        # the h3 index of the window itself. will have a lower resolution then the h3_resolution
        # requested for the window
        # print(resultset.window_index, h3.h3_get_resolution(resultset.window_index))

        # the h3indexes as used for the query
        # print(resultset.h3indexes_queried)

        # get as a pandas dataframe. This will move the data, so the resultset will be empty afterwards
        detections_df = resultset.to_dataframe()
        # print(detections_df)

        recording_timestamps = [datetime.utcfromtimestamp(ts.astype('O') / 1e9) for ts in
                                detections_df.recorded_at.unique()]

        # to get missing values when there have been no detections, we must generate all timestamps when a index
        # has been covered by a scene - they are not stored. We just use the scene footprints to generate our subset of
        # h3indexes for each scene covering a h3index
        indexes_found = detections_df.h3index.unique()
        query_polygon = h3cpy.h3indexes_convex_hull(indexes_found)

        # print(query_polygon.to_geojson_str())

        scene_h3indexes_df = fetch_using_intersecting_h3indexes(
            postgres_cur,
            indexes_found,
            # just query for the h3index where we got data from clickhouse for. thats all we need to find holes in the timeseries
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
        if scene_h3indexes_df.empty:  # TODO
            continue

        # print(scene_h3indexes_df)

        # join the two dataframes to get a time series
        # cut of the timezone first, its UTC anyways. TODO: improve this
        scene_h3indexes_df['recorded_at'] = scene_h3indexes_df['recorded_at'].dt.tz_localize(None)
        scene_h3indexes_df['processed_at'] = scene_h3indexes_df['processed_at'].dt.tz_localize(None)
        joined_df = pd.merge(scene_h3indexes_df, detections_df,
                             how="left",
                             on=['h3index', 'recorded_at', 'processed_at', 'sensor_id', 'processor_id']
                             )
        joined_df.sort_values(by=["h3index", "recorded_at"], inplace=True)
        print(joined_df)


def main():
    aoi_geom = shape(json.loads(AOI))
    if MAX_WORKERS > 1:

        # let the kernel immediately kill all child processes on Ctrl-C
        import signal
        signal.signal(signal.SIGINT, signal.SIG_DFL)

        # split the AOI into chunks to distribute these accross multiple processes
        polygon_chunks = chunk_polygon(aoi_geom, num_chunks_approx=MAX_WORKERS * 2)

        with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            executor.map(process_window, polygon_chunks)
    else:
        # using just a single process
        process_window(aoi_geom)


if __name__ == "__main__":
    main()

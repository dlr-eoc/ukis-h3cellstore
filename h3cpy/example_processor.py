"""
This example is a possible blueprint for a simple processor crawling through the data
using multiple sliding windows in multiple processes

This processor requires a few additional packages:

* `pip install --upgrade pip`
* `psycopg2` to connect to postgres. the `psycopg2` package requires compilation, use
   the `pyscopg2-binary` package  for precompiled bindings.
* `h3ronpy` for polygon smoothing. (at least v0.7.1)
   install via `pip install -i https://eoc-gzs-db01-vm.eoc.dlr.de:8080/repository/py-all/simple h3ronpy>=0.7.1`


Additionally, this processor requires three db connections:

* One for the results, this may be any local, PostGIS-enabled Postgres DB
* One to clickhouse containing the water data
* One to a postgres installation containing the metadata for the water data

The last two require an SSH tunnel to the machine hosting these two DBs:

```
ssh your-user-name@torvalds.eoc.dlr.de -L 9010:localhost:9010 -L 5433:localhost:5432
```

Credentials for the Postgres metadata db are in the password database.
"""

import concurrent.futures
import h3.api.numpy_int as h3
import h3ronpy
import json
import pandas as pd
import psycopg2
import shapely.wkb
from datetime import datetime
from shapely.geometry import shape, Polygon

import h3cpy
from h3cpy.concurrent import chunk_polygon
from h3cpy.postgres import fetch_using_intersecting_h3indexes

# number of worker processes to use, set to 1 to skip parallelization and
# gain better debuggability
MAX_WORKERS = 1

# postgres credentials see password db, here they are passed via PGUSER
# and PGPASSWORD environment variables (in libpq)
DSN_POSTGRES = "dbname=water2 host=127.0.0.1 port=5433"
DSN_CLICKHOUSE = "tcp://localhost:9010/water2?compression=lz4"
DSN_POSTGRES_OUTPUT = "user=mand_nc host=127.0.0.1 password=xxxx dbname=water_out"

# polygon geometry to visit
AOI = """
{
        "type": "Polygon",
        "coordinates": [
          [
            [
              11.086235046386719,
              47.97705279557946
            ],
            [
              11.287422180175781,
              47.97705279557946
            ],
            [
              11.287422180175781,
              48.09000531373827
            ],
            [
              11.086235046386719,
              48.09000531373827
            ],
            [
              11.086235046386719,
              47.97705279557946
            ]
          ]
        ]
      }
"""


def create_output_schema():
    postgres_conn = psycopg2.connect(DSN_POSTGRES_OUTPUT)
    postgres_cur = postgres_conn.cursor()
    postgres_cur.execute("""
    create table if not exists water_results (
        id serial primary key,
        window_h3index text,
        geom geometry(Polygon, 4326)
    )
    """)

    # create an index, this slows down inserts, but allows to view
    # the contents while the processor is running with qgis
    postgres_cur.execute("""
    create index if not exists water_results_geom_idx on water_results using gist(geom);
    """)
    postgres_conn.commit()
    postgres_conn.close()


def process_window(window_geom: Polygon):
    # connect to postgres for metadata
    postgres_meta_conn = psycopg2.connect(DSN_POSTGRES)
    postgres_meta_cur = postgres_meta_conn.cursor()

    # connect to clickhouse
    clickhouse_conn = h3cpy.ClickhouseConnection(DSN_CLICKHOUSE)
    tablesets = clickhouse_conn.list_tablesets()

    # connect to postgres for output
    postgres_output_conn = psycopg2.connect(DSN_POSTGRES_OUTPUT)
    postgres_output_cur = postgres_output_conn.cursor()

    # print all tablesets found
    for tsname, ts in tablesets.items():
        print(f"tableset {tsname} found")
        # print(ts.base_resolutions)
        # print(ts.compacted_resolutions)
        # print(ts.columns)

    # query to clickhouse, you can also create a prefetch template with a query against a higher resolution to
    # determine whether it is worth fetching. This might be interesting when you are fetching nodata columns as well,
    # they are only interesting when there is not exclusively nodata.
    querystring_template = """
    select scene as scene_id,
        h3index, 
        recorded_at,
        area_percent_water_class_090_100, 
        area_percent_water_class_080_090, 
        area_percent_water_class_070_080
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
    for resultset in clickhouse_conn.window_iter(window_geom, tablesets["water"], 13, window_max_size=20000,
                                                 querystring_template=querystring_template,
                                                 prefetch_querystring_template=querystring_template,):

        # the h3 index of the window itself. will have a lower resolution then the h3_resolution
        # requested for the window
        # print(resultset.window_index, h3.h3_get_resolution(resultset.window_index))

        # the h3indexes as used for the query
        # print(resultset.h3indexes_queried)

        # get as a pandas dataframe. This will move the data, so the resultset will be empty afterwards
        detections_df = resultset.to_dataframe()

        recording_timestamps = [datetime.utcfromtimestamp(ts.astype('O') / 1e9) for ts in
                                detections_df.recorded_at.unique()]

        # to get missing values when there have been no detections, we must generate all timestamps when a index
        # has been covered by a scene - they are not stored. We just use the scene footprints to generate our subset of
        # h3indexes for each scene covering a h3index
        indexes_found = detections_df.h3index.unique()
        query_polygon = h3cpy.h3indexes_convex_hull(indexes_found)

        scene_h3indexes_df = fetch_using_intersecting_h3indexes(
            postgres_meta_cur,
            indexes_found,
            # just query for the h3index where we got data from clickhouse for.
            # That's all we need to find holes in the timeseries
            "wkb_geom",
            """
            select
                s.id as scene_id,
                s.recorded_at,
                st_asbinary(st_force2d(s.footprint)) wkb_geom 
            from scene s 
            where st_intersects(s.footprint, st_geomfromwkb(%s, 4326))
                and s.recorded_at = any(%s)
            """,
            (query_polygon.to_wkb(), recording_timestamps)
        )
        if scene_h3indexes_df.empty:  # TODO
            print("skip")
            continue

        # join the two dataframes to get a time series
        # cut of the timezone first, its UTC anyways. TODO: improve this
        scene_h3indexes_df['recorded_at'] = scene_h3indexes_df['recorded_at'].dt.tz_localize(None)
        joined_df = pd.merge(scene_h3indexes_df, detections_df,
                             how="left",
                             on=['scene_id', 'h3index', 'recorded_at']
                             )
        joined_df.sort_values(by=["h3index", "recorded_at"], inplace=True)

        # do some analysis:

        # nan for area percent means that there was no detection -> setting to 0.0
        joined_df.fillna(value={"area_percent_water_class_070_080": 0.0,
                                "area_percent_water_class_080_090": 0.0,
                                "area_percent_water_class_090_100": 0.0},
                         inplace=True)

        # randomly assigned weights, we can also just add to create a similar effect of what we are used to
        joined_df["water_certainty"] = (1.5 * joined_df["area_percent_water_class_090_100"]) \
                                       + (.85 * joined_df["area_percent_water_class_080_090"]) \
                                       + (.75 * joined_df["area_percent_water_class_070_080"])

        # when we fetched nodata before would now be a good moment to take these values out again so they do not
        # mess up the mean()

        rfreq = joined_df[["h3index", "water_certainty"]].groupby(["h3index"]).mean()
        rfreq_threshold = 0.8

        water_h3indexes = rfreq[rfreq.water_certainty >= rfreq_threshold].index.to_numpy(dtype="uint64")
        window_index_str = h3.h3_to_string(resultset.window_index)
        if water_h3indexes.size != 0:
            window_index_str = h3.h3_to_string(resultset.window_index)
            polygons = h3ronpy.Polygon.from_h3indexes_aligned(water_h3indexes,
                                                              h3.h3_get_resolution(resultset.window_index),
                                                              smoothen=True)

            print(f"Found {len(polygons)} polygons in {window_index_str}")
            for poly in polygons:
                postgres_output_cur.execute("""
                insert into water_results (window_h3index, geom) select %s, st_geomfromwkb(%s, 4326)
                """, (
                    window_index_str,
                    psycopg2.Binary(shapely.wkb.dumps(shape(poly)))
                ))
            postgres_output_conn.commit()
        else:
            print(f"Found no polygons in {window_index_str}")


def main():
    aoi_geom = shape(json.loads(AOI))
    create_output_schema()

    if MAX_WORKERS > 1:

        # let the kernel immediately kill all child processes on Ctrl-C
        import signal
        signal.signal(signal.SIGINT, signal.SIG_DFL)

        # split the AOI into chunks to distribute these accross multiple processes
        polygon_chunks = chunk_polygon(aoi_geom, num_chunks_approx=MAX_WORKERS * 2)
        print(f"split the polygon into {len(polygon_chunks)} chunks")

        with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            executor.map(process_window, polygon_chunks)
    else:
        # using just a single process
        process_window(aoi_geom)


if __name__ == "__main__":
    main()

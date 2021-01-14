import h3cpy

conn = h3cpy.ClickhouseConnection("tcp://localhost:9010/water2?compression=lz4")
tablesets = conn.list_tablesets()

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
for resultset in conn.window_iter(geom, tablesets["water"], 10, window_max_size=6000):
    print(resultset.window_index)
    # get as a pandas dataframe. This will move the data, so the resultset will be empty afterwards
    df = resultset.to_dataframe()
    print(df)

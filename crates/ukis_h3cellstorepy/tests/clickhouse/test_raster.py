import numpy as np
from ukis_h3cellstorepy.clickhouse import CompactedTableSchemaBuilder, GRPCConnection, TableSetQuery

# noinspection PyUnresolvedReferences
from ..fixtures import clickhouse_grpc_endpoint, rasterio, clickhouse_testdb_name, testdata_path, pd


def get_schema(tableset_name, h3_res):
    csb = CompactedTableSchemaBuilder(tableset_name)
    csb.h3_base_resolutions(list(range(0, h3_res + 1)))
    csb.temporal_resolution("second")

    csb.table_engine("ReplacingMergeTree")

    csb.add_h3index_column("h3index")
    csb.add_aggregated_column("is_water", "Float32", "RelativeToCellArea")

    schema = csb.build()
    return schema


def test_ingest_raster_is_covered(testdata_path, rasterio, clickhouse_grpc_endpoint, clickhouse_testdb_name, pd):
    """ingest a raster file and test if it still covers the same area after aggregation"""
    tableset_name = "from_raster"
    h3_res = 12
    with rasterio.open(testdata_path / "S2B_MSIL1C_20181011T101019_N0206_R022_T32TPT_20181011T122059_WATER.tif") as msk:
        mask = msk.read(1)

        from h3ronpy.raster import raster_to_dataframe
        df_in = raster_to_dataframe(mask, msk.transform, h3_res, compacted=True, geo=False)
        df_in.rename(columns={"value": "is_water"}, inplace=True)
        df_in["is_water"] = df_in["is_water"].astype('float')

        con = GRPCConnection(clickhouse_grpc_endpoint, clickhouse_testdb_name, create_db=True)
        con.drop_tableset(tableset_name)  # just to be sure that its empty
        schema = get_schema(tableset_name, h3_res)
        con.create_tableset(schema)
        con.insert_h3dataframe_into_tableset(schema, df_in)
        print(con.tableset_stats(tableset_name).to_pandas())

        # save for debugging
        from h3ronpy.util import dataframe_to_geodataframe
        # dataframe_to_geodataframe(df_in).to_file("/tmp/in_df.gpkg", driver="GPKG")


        # reduce the number of h3indexes to pass to clickhouse by converting to a lower resolution
        from h3ronpy.op import change_resolution
        aoi_h3indexes = np.unique(change_resolution(df_in["h3index"].to_numpy(), h3_res - 5))

        traverser = con.traverse_tableset_area_of_interest(
            tableset_name,
            TableSetQuery(),
            aoi_h3indexes,
            h3_res - 1,  # one resolution less to use one of the aggregated resolutions
        )

        fetched_df = pd.concat([df.to_pandas() for df in traverser])
        fetched_df = fetched_df[fetched_df.is_water > 0.0]

        fetched_geo_df = dataframe_to_geodataframe(fetched_df)

        from rasterio.features import rasterize
        rasterized = rasterize([(s, int(v * 255)) for s, v in zip(fetched_geo_df.geometry, fetched_geo_df.is_water)],
                               out_shape=mask.shape, transform=msk.transform, fill=0)

        profile = msk.profile
        profile.update({
            "width": rasterized.shape[1],
            "height": rasterized.shape[0]
        })
        # with rasterio.open("/tmp/rasterized.tif", "w", **profile) as dest:
        #   dest.write(rasterized, 1)

        missed_values = np.where(rasterized == 0, mask,
                                 0)  # contains only pixels not covered by the df fetched from the db

        # with rasterio.open("/tmp/mask-rasterized-diff.tif", "w", **profile) as dest:
        #    dest.write(missed_values, 1)

        num_missed_values = np.count_nonzero(missed_values)
        num_values = missed_values.size
        assert float(num_missed_values) / float(num_values) < 0.005, f"{num_missed_values} of {num_values} got lost"


def test_ingest_raster_agg_rta(testdata_path, rasterio, clickhouse_grpc_endpoint, clickhouse_testdb_name, pd):
    """ingest a raster file and test if aggregation RelativeToArea"""
    tableset_name = "from_raster"
    h3_res = 12
    with rasterio.open(testdata_path / "S2B_MSIL1C_20181011T101019_N0206_R022_T32TPT_20181011T122059_WATER.tif") as msk:
        mask = msk.read(1)

        from h3ronpy.raster import raster_to_dataframe
        df_in = raster_to_dataframe(mask, msk.transform, h3_res, compacted=True, geo=False)
        df_in.rename(columns={"value": "is_water"}, inplace=True)
        df_in["is_water"] = df_in["is_water"].astype('float')

        con = GRPCConnection(clickhouse_grpc_endpoint, clickhouse_testdb_name, create_db=True)
        con.drop_tableset(tableset_name)  # just to be sure that its empty
        schema = get_schema(tableset_name, h3_res)
        con.create_tableset(schema)
        con.insert_h3dataframe_into_tableset(schema, df_in)

        # reduce the number of h3indexes to pass to clickhouse by converting to a lower resolution
        from h3ronpy.op import change_resolution
        aoi_h3indexes = np.unique(change_resolution(df_in["h3index"].to_numpy(), h3_res - 5))

        traverser = con.traverse_tableset_area_of_interest(
            tableset_name,
            TableSetQuery(),
            aoi_h3indexes,
            h3_res - 3,
        )

        fetched_df = pd.concat([df.to_pandas() for df in traverser])
        fetched_df = fetched_df[fetched_df.is_water > 0.0]

        # values above 1.0 are not possible and should not exist
        assert (fetched_df.is_water > 1.0).value_counts().get(True, 0) == 0, "found values > 1.0"




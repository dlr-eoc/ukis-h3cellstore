# noinspection PyUnresolvedReferences
from h3cellstorepy.clickhouse import TableSetQuery
from .test_schema import setup_elephant_schema_with_data
# noinspection PyUnresolvedReferences
from ..fixtures import clickhouse_grpc_endpoint, pl, clickhouse_testdb_name, geojson


def test_traverse_by_geometry(clickhouse_grpc_endpoint, clickhouse_testdb_name, pl, geojson):
    with setup_elephant_schema_with_data(clickhouse_grpc_endpoint, clickhouse_testdb_name, pl) as ctx:
        coord_diff = 1.0
        geom = geojson.loads(f"""{{
            "type": "Polygon",
            "coordinates": [
              [
                [{ctx.center_point[0] - coord_diff}, {ctx.center_point[1] - coord_diff}],
                [{ctx.center_point[0] + coord_diff}, {ctx.center_point[1] - coord_diff}],
                [{ctx.center_point[0] + coord_diff}, {ctx.center_point[1] + coord_diff}],
                [{ctx.center_point[0] - coord_diff}, {ctx.center_point[1] + coord_diff}],
                [{ctx.center_point[0] - coord_diff}, {ctx.center_point[1] - coord_diff}]
              ]
            ]
          }}""")
        traverser = ctx.con.traverse_tableset_area_of_interest(
            ctx.schema.name,
            TableSetQuery(),
            geom,
            ctx.schema.max_h3_resolution
        )
        assert traverser.traversal_h3_resolution < ctx.schema.max_h3_resolution
        assert len(traverser) > 0
        assert len(traverser) < len(ctx.df)

        dfs_found = 0
        for dataframe_wrapper in traverser:
            df = dataframe_wrapper.to_polars()
            assert len(df) > 0
            dfs_found += 1
            # print(df)
        assert dfs_found > 0
        assert dfs_found <= len(traverser)


def test_traverse_by_cells(clickhouse_grpc_endpoint, clickhouse_testdb_name, pl):
    with setup_elephant_schema_with_data(clickhouse_grpc_endpoint, clickhouse_testdb_name, pl) as ctx:
        traverser = ctx.con.traverse_tableset_area_of_interest(
            ctx.schema.name,
            TableSetQuery(),
            ctx.df["h3index"].to_numpy(),
            ctx.schema.max_h3_resolution
        )
        assert traverser.traversal_h3_resolution < ctx.schema.max_h3_resolution
        assert len(traverser) > 0
        assert len(traverser) < len(ctx.df)

        dfs_found = 0
        for dataframe_wrapper in traverser:
            df = dataframe_wrapper.to_polars()
            assert len(df) > 0
            dfs_found += 1
            # print(df)
        assert dfs_found > 0
        assert dfs_found <= len(traverser)


def test_traverse_by_cells_with_filter(clickhouse_grpc_endpoint, clickhouse_testdb_name, pl):
    with setup_elephant_schema_with_data(clickhouse_grpc_endpoint, clickhouse_testdb_name, pl) as ctx:
        traverser = ctx.con.traverse_tableset_area_of_interest(
            ctx.schema.name,
            TableSetQuery(),
            ctx.df["h3index"].to_numpy(),
            ctx.schema.max_h3_resolution,
            # filter to rule out any results for this unittest
            filter_query=TableSetQuery.from_template("select h3index from <[table]> where false")
        )
        assert traverser.traversal_h3_resolution < ctx.schema.max_h3_resolution
        assert len(traverser) > 0
        assert len(traverser) < len(ctx.df)

        dfs_found = 0
        for dataframe_wrapper in traverser:
            df = dataframe_wrapper.to_polars()
            assert len(df) > 0
            dfs_found += 1
            # print(df)
        assert dfs_found == 0

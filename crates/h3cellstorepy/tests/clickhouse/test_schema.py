import json

import pytest
from h3cellstorepy.clickhouse import CompactedTableSchemaBuilder, CompactedTableSchema, TableSetQuery, CompressionMethod
from . import setup_elephant_schema_with_data, elephant_schema

# noinspection PyUnresolvedReferences
from ..fixtures import clickhouse_grpc_endpoint, pl, clickhouse_testdb_name


def test_multi_year_partitioning():
    elephant_schema(temporal_partitioning="5 years")  # should not raise
    with pytest.raises(ValueError):
        elephant_schema(temporal_partitioning="0 years")
    with pytest.raises(ValueError):
        elephant_schema(temporal_partitioning="z years")


def test_schema_save_and_load():
    tableset_name, schema = elephant_schema()
    sqls_before = schema.sql_statements()
    assert type(sqls_before) == list
    assert len(sqls_before) > 0
    assert len(sqls_before[0]) > 10

    schema_description = schema.to_json_string()
    json.loads(schema_description)  # should not fail

    # create a new schema-objects from the serialized representation
    schema2 = CompactedTableSchema.from_json_string(schema_description)
    sqls_after = schema2.sql_statements()
    assert sqls_before == sqls_after


def test_schema_h3_partitioning_lower_resolution(clickhouse_grpc_endpoint, clickhouse_testdb_name, pl):
    with setup_elephant_schema_with_data(clickhouse_grpc_endpoint, clickhouse_testdb_name, pl,
                                         h3_partitioning='lower_resolution', resolution_difference=7) as ctx:
        pass


def test_schema_create_and_fill(clickhouse_grpc_endpoint, clickhouse_testdb_name, pl):
    with setup_elephant_schema_with_data(clickhouse_grpc_endpoint, clickhouse_testdb_name, pl) as ctx:
        # read from db again. un-compaction is performed automatically
        queried_df = ctx.con.query_tableset_cells(ctx.schema.name, TableSetQuery(), ctx.disk,
                                                  ctx.schema.max_h3_resolution).to_polars()
        assert queried_df.shape == ctx.df.shape

        # it is also possible to load the data on a lower resolution. the cells in `disk` get automatically transformed
        # to the requested h3 resolution
        queried_lower_df = ctx.con.query_tableset_cells(ctx.schema.name, TableSetQuery(), ctx.disk,
                                                        ctx.schema.max_h3_resolution - 2).to_polars()
        assert ctx.df.shape[0] > queried_lower_df.shape[0]
        assert ctx.df.shape[1] == queried_lower_df.shape[1]


def test_schema_create_and_fill_templatedquery(clickhouse_grpc_endpoint, clickhouse_testdb_name, pl):
    with setup_elephant_schema_with_data(clickhouse_grpc_endpoint, clickhouse_testdb_name, pl) as ctx:
        queried_lower_df = ctx.con.query_tableset_cells(ctx.schema.name, TableSetQuery.from_template(
            "select * from <[table]> where elephant_density < 2"), ctx.disk,
                                                        ctx.schema.max_h3_resolution).to_polars()
        assert queried_lower_df.shape[0] == 0

        queried_lower_df = ctx.con.query_tableset_cells(ctx.schema.name, TableSetQuery.from_template(
            "select * from <[table]> where (rand() % 2) = 0"), ctx.disk,
                                                        ctx.schema.max_h3_resolution).to_polars()
        assert ctx.df.shape[0] > queried_lower_df.shape[0]
        assert ctx.df.shape[1] == queried_lower_df.shape[1]

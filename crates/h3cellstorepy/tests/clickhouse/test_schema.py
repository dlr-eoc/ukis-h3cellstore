import json

import pytest
from h3cellstorepy.clickhouse import CompactedTableSchemaBuilder, CompactedTableSchema, TableSetQuery, CompressionMethod
from h3cellstorepy.clickhouse import connect

import h3.api.numpy_int as h3
import numpy as np
import contextlib

# noinspection PyUnresolvedReferences
from ..fixtures import clickhouse_grpc_endpoint, pl, clickhouse_testdb_name


def elephant_schema(tableset_name="okavango_delta", temporal_partitioning="month"):
    csb = CompactedTableSchemaBuilder(tableset_name)
    csb.h3_base_resolutions(list(range(0, 8)))
    csb.temporal_resolution("second")
    csb.temporal_partitioning(temporal_partitioning)
    csb.add_column("is_valid", "UInt8", None, CompressionMethod("gorilla"))
    csb.add_aggregated_column("elephant_density", "Float32", "RelativeToCellArea")
    schema = csb.build()  # raises when the schema is invalid / missing something
    assert schema is not None
    #print(schema.to_json_string())
    #print(schema.sql_statements())
    return tableset_name, schema


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


@contextlib.contextmanager
def setup_elephant_schema_with_data(clickhouse_grpc_endpoint, clickhouse_testdb_name, pl):
    tableset_name, schema = elephant_schema()
    con = connect(clickhouse_grpc_endpoint, clickhouse_testdb_name, create_db=True)
    con.drop_tableset(schema.name)
    con.create_tableset(schema)
    assert schema.name in con.list_tablesets()

    # uncompacted disk
    disk = h3.k_ring(h3.geo_to_h3(20.0, 10.0, schema.max_h3_resolution), 10).astype(np.uint64)
    df = pl.DataFrame({
        "h3index": disk,
        "is_valid": np.ones(len(disk)),
        "elephant_density": np.ones(len(disk)) * 4
    })

    # write to db - this performs auto-compaction
    con.insert_h3dataframe_into_tableset(schema, df)

    yield con, schema, disk, df

    con.drop_tableset(schema.name)
    assert schema.name not in con.list_tablesets()


def test_schema_create_and_fill(clickhouse_grpc_endpoint, clickhouse_testdb_name, pl):
    with setup_elephant_schema_with_data(clickhouse_grpc_endpoint, clickhouse_testdb_name, pl) as (
            con, schema, disk, df):
        # read from db again. un-compaction is performed automatically
        queried_df = con.query_tableset_cells(schema.name, TableSetQuery(), disk, schema.max_h3_resolution).to_polars()
        assert queried_df.shape == df.shape

        # it is also possible to load the data on a lower resolution. the cells in `disk` get automatically transformed
        # to the requested h3 resolution
        queried_lower_df = con.query_tableset_cells(schema.name, TableSetQuery(), disk,
                                                    schema.max_h3_resolution - 2).to_polars()
        assert df.shape[0] > queried_lower_df.shape[0]
        assert df.shape[1] == queried_lower_df.shape[1]


def test_schema_create_and_fill_templatedquery(clickhouse_grpc_endpoint, clickhouse_testdb_name, pl):
    with setup_elephant_schema_with_data(clickhouse_grpc_endpoint, clickhouse_testdb_name, pl) as (
            con, schema, disk, df):
        queried_lower_df = con.query_tableset_cells(schema.name, TableSetQuery.from_template(
            "select * from <[table]> where elephant_density < 2"), disk,
                                                    schema.max_h3_resolution).to_polars()
        assert queried_lower_df.shape[0] == 0

        queried_lower_df = con.query_tableset_cells(schema.name, TableSetQuery.from_template(
            "select * from <[table]> where (rand() % 2) = 0"), disk,
                                                    schema.max_h3_resolution).to_polars()
        assert df.shape[0] > queried_lower_df.shape[0]
        assert df.shape[1] == queried_lower_df.shape[1]

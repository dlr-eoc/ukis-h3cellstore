from ukis_h3cellstorepy.clickhouse import CompactedTableSchemaBuilder, CompactedTableSchema, CompressionMethod, GRPCConnection

import h3.api.numpy_int as h3
import numpy as np
import contextlib


def elephant_schema(tableset_name="okavango_delta", temporal_partitioning="month", h3_partitioning="basecell", **kw):
    csb = CompactedTableSchemaBuilder(tableset_name)
    csb.h3_base_resolutions(list(range(0, 8)))
    csb.temporal_resolution("second")
    csb.temporal_partitioning(temporal_partitioning)
    csb.h3_partitioning(h3_partitioning, **kw)
    csb.add_column("is_valid", "UInt8", compression_method=CompressionMethod("gorilla"))
    csb.add_aggregated_column("elephant_density", "Float32", "RelativeToCellArea")
    csb.add_aggregated_column("some_category", "UInt8", "SetNullOnConflict", nullable=True)
    schema = csb.build()  # raises when the schema is invalid / missing something
    assert schema is not None
    #print(schema.to_json_string())
    #print(schema.sql_statements())
    assert "Gorilla" in schema.sql_statements()[0]

    return tableset_name, schema


class SchemaContext:
    con: GRPCConnection = None
    schema: CompactedTableSchema = None,
    disk: np.array = None
    df: 'pl.DataFrame' = None
    center_point = None


@contextlib.contextmanager
def setup_elephant_schema_with_data(clickhouse_grpc_endpoint, clickhouse_testdb_name, pl, **kw):
    tableset_name, schema = elephant_schema(**kw)
    con = GRPCConnection(clickhouse_grpc_endpoint, clickhouse_testdb_name, create_db=True)
    con.drop_tableset(schema.name)
    con.create_tableset(schema)
    assert schema.name in con.list_tablesets()

    center_point = (20.0, 10.0)
    # uncompacted disk
    disk = h3.k_ring(h3.geo_to_h3(center_point[1], center_point[0], schema.max_h3_resolution), 10).astype(np.uint64)
    cat1 = np.ones(int(len(disk) / 2)) * 23
    cat2 = np.ones(len(disk)- len(cat1)) * 12
    df = pl.DataFrame({
        "h3index": disk,
        "is_valid": np.ones(len(disk)),
        "elephant_density": np.ones(len(disk)) * 4,
        "some_category": np.concatenate((cat1, cat2))
    })

    # write to db - this performs auto-compaction
    con.insert_h3dataframe_into_tableset(schema, df)

    context = SchemaContext()
    context.con = con
    context.schema = schema
    context.disk = disk
    context.df = df
    context.center_point = center_point
    yield context

    con.drop_tableset(schema.name)
    assert schema.name not in con.list_tablesets()

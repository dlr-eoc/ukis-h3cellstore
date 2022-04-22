import json

import pytest
from h3cellstorepy.clickhouse import CompactedTableSchemaBuilder, CompactedTableSchema


def elephant_schema(tableset_name="okavango_delta", temporal_partitioning="month"):
    csb = CompactedTableSchemaBuilder(tableset_name)
    csb.h3_base_resolutions(list(range(0, 8)))
    csb.temporal_resolution("second")
    csb.temporal_partitioning(temporal_partitioning)
    csb.add_h3index_column("migrating_from")
    csb.add_column("is_valid", "UInt8")
    csb.add_aggregated_column("elephant_density", "Float32", "RelativeToCellArea")
    schema = csb.build()  # raises when the schema is invalid / missing something
    assert schema is not None
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

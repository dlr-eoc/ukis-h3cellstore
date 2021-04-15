import json

from bamboo_h3.schema import CompactedTableSchemaBuilder, Schema

# noinspection PyUnresolvedReferences
from .fixtures import clickhouse_db, naturalearth_africa_dataframe_4


def elephant_schema(tableset_name="okavango_delta"):
    csb = CompactedTableSchemaBuilder(tableset_name)
    csb.h3_base_resolutions(list(range(0, 8)))
    csb.temporal_resolution("second")
    csb.temporal_partitioning("month")
    csb.add_h3index_column("migrating_from")
    csb.add_column("is_valid", "u8")
    csb.add_aggregated_column("elephant_density", "f32", "RelativeToCellArea")
    schema = csb.build()  # raises when the schema is invalid / missing something
    assert schema is not None
    return tableset_name, schema


def test_create_and_delete_schema(clickhouse_db):
    tableset_name, schema = elephant_schema()
    clickhouse_db.create_schema(schema)
    tableset = clickhouse_db.list_tablesets().get(tableset_name)
    assert tableset is not None

    clickhouse_db.drop_tableset(tableset)
    tableset = clickhouse_db.list_tablesets().get(tableset_name)
    assert tableset is None


def test_schema_save_and_load():
    tableset_name, schema = elephant_schema()
    sqls_before = schema.sql_statements()
    assert type(sqls_before) == list
    assert len(sqls_before) > 0
    assert len(sqls_before[0]) > 10

    schema_description = schema.to_json_string()
    json.loads(schema_description)  # should not fail

    # create a new schema-objects from the serialized representation
    schema2 = Schema.from_json_string(schema_description)
    sqls_after = schema2.sql_statements()
    assert sqls_before == sqls_after


def test_write_dataframe(naturalearth_africa_dataframe_4):
    print(naturalearth_africa_dataframe_4)
    # TODO

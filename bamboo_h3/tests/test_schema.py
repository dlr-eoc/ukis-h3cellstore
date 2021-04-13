# noinspection PyUnresolvedReferences
from .fixtures import clickhouse_db

from bamboo_h3.schema import CompactedTableSchemaBuilder


def elephant_schema(tableset_name="okavango_delta"):
    csb = CompactedTableSchemaBuilder(tableset_name)
    csb.h3_base_resolutions(list(range(0, 8)), compacted=True)
    csb.temporal_resolution("second")
    csb.temporal_partitioning("month")
    csb.add_h3index_column("migrating_from")
    csb.add_column("is_valid", "u8")
    csb.add_aggregated_column("elephant_density", "f32", "RelativeToCellArea")
    return tableset_name, csb.build()  # raises when the schema is invalid / missing something


def test_create_and_delete_schema(clickhouse_db):
    tableset_name, schema = elephant_schema()
    clickhouse_db.create_schema(schema)
    tableset = clickhouse_db.list_tablesets().get(tableset_name)
    assert tableset is not None

    clickhouse_db.drop_tableset(tableset)
    tableset = clickhouse_db.list_tablesets().get(tableset_name)
    assert tableset is None

"""
Database schema creation
========================

Defining a schema
-----------------

>>> from bamboo_h3.clickhouse.schema import CompactedTableSchemaBuilder
>>> csb = CompactedTableSchemaBuilder("okavango_delta")
>>> csb.h3_base_resolutions(list(range(0, 8)))
>>> csb.temporal_resolution("second")
>>> csb.temporal_partitioning("month")
>>> csb.add_h3index_column("migrating_from")
>>> csb.add_column("is_valid", "u8")
>>> csb.add_aggregated_column("elephant_density", "f32", "RelativeToCellArea")
>>> schema = csb.build() # raises when the schema is invalid / missing something
>>> #print(schema.to_json_string())

"""

from ..bamboo_h3 import \
    Schema, \
    CompactedTableSchemaBuilder

__all__ = [
    Schema.__name__,
    CompactedTableSchemaBuilder.__name__,
]

if __name__ == "__main__":
    # run doctests
    import doctest

    doctest.testmod(verbose=True)

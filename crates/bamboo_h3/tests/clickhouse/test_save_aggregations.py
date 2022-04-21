import h3.api.numpy_int as h3
import numpy as np
import pandas as pd
import pytest
from bamboo_h3.clickhouse.schema import CompactedTableSchemaBuilder

# noinspection PyUnresolvedReferences
from ..fixtures import clickhouse_db


def some_dataframe():
    max_res = 1
    start_index = h3.geo_to_h3(33.90371, -0.95000, 0)
    children = h3.h3_to_children(start_index, max_res)
    df = pd.DataFrame({
        "h3index": np.asarray([children[0], children[4]], dtype=np.uint64),
        "value": np.asarray([5.0, 2.0], dtype=np.float32)
    })
    return max_res, start_index, df


def some_schema(tableset_name, max_res, value_agg_method):
    csb = CompactedTableSchemaBuilder(tableset_name)
    csb.h3_base_resolutions(list(range(0, max_res + 1)))
    csb.add_aggregated_column("value", "f32", value_agg_method)
    schema = csb.build()  # raises when the schema is invalid / missing something
    return schema


@pytest.mark.parametrize("agg_method,expected",
                         [("sum", 7.0), ("min", 2.0), ("max", 5.0), ("avg", 3.5), ("relativetocellarea", 1.0)])
def test_aggregated_float_column(clickhouse_db, agg_method, expected):
    tableset_name = "agg_tableset"
    max_res, start_index, dataframe = some_dataframe()
    schema = some_schema(tableset_name, max_res, agg_method)
    try:
        clickhouse_db.save_dataframe(schema, dataframe)

        other_res_df = clickhouse_db.tableset_fetch(
            tableset_name,
            np.asarray([start_index, ], dtype=np.uint64)
        ).to_dataframe()
        assert len(other_res_df) == 1
        assert other_res_df.value[0] == pytest.approx(expected, 0.0001)
    finally:
        clickhouse_db.drop_tableset(tableset_name)

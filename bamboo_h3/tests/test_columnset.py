import h3.api.numpy_int as h3
import numpy as np
import pandas as pd
from bamboo_h3 import ColumnSet

# noinspection PyUnresolvedReferences
from .fixtures import naturalearth_africa_dataframe_4, r_tiff_dataframe_uncompacted_8, naturalearth_africa_dataframe_8


def test_to_compacted_single_column(naturalearth_africa_dataframe_4):
    columnset = ColumnSet.from_dataframe(
        naturalearth_africa_dataframe_4.loc[:, ['h3index']]  # drop all columns except the h3index
    )
    columnset_compacted = columnset.to_compacted()
    assert not columnset_compacted.empty
    assert len(columnset) > len(columnset_compacted)

    compaction_percent = float(len(columnset_compacted)) / len(columnset)
    print(f"[compacted to {compaction_percent * 100:.2f}%]", end=" ")


def test_to_compacted_multiple_column_one_value_one_row():
    start_index = 596353829637718015
    h3index_array = h3.h3_to_children(start_index, res=h3.h3_get_resolution(start_index) + 5)
    num_elements = len(h3index_array)
    value = 23
    assert num_elements > 10000
    columnset = ColumnSet.from_dataframe(
        pd.DataFrame({
            "h3index": h3index_array,
            "value": np.full((num_elements,), value, dtype=np.uint8)
        })
    )
    assert len(columnset) == num_elements
    columnset_compacted = columnset.to_compacted()
    assert not columnset_compacted.empty
    assert len(columnset_compacted) == 1
    df_out = columnset_compacted.to_dataframe()
    assert len(df_out) == 1
    assert df_out["value"][0] == value


def test_to_compacted_multiple_column_few_values_few_rows():
    start_index = 596353829637718015
    h3index_array = h3.h3_to_children(start_index, res=h3.h3_get_resolution(start_index) + 5)
    num_elements = len(h3index_array)
    value = 23
    assert num_elements > 10000

    values = np.full((num_elements,), value, dtype=np.uint8)
    values[4] = 12  # change one value, "compactability" is worse now

    columnset = ColumnSet.from_dataframe(
        pd.DataFrame({
            "h3index": h3index_array,
            "value": values
        })
    )
    assert len(columnset) == num_elements
    columnset_compacted = columnset.to_compacted()
    assert not columnset_compacted.empty
    assert len(columnset_compacted) > 1
    assert len(columnset_compacted) < num_elements

    df_out = columnset_compacted.to_dataframe()
    assert len(df_out) > 1
    assert len(df_out) < num_elements

    vc = df_out.value.value_counts()
    assert len(vc) == 2
    assert vc[12] == 1
    assert vc[value] > 1


def test_to_compacted_multiple_columns(naturalearth_africa_dataframe_4):
    columnset = ColumnSet.from_dataframe(
        naturalearth_africa_dataframe_4.loc[:, ['h3index', 'pop_est', 'gdp_md_est']]  # drop all columns except these
    )
    columnset_compacted = columnset.to_compacted()
    assert not columnset_compacted.empty
    assert len(columnset) > len(columnset_compacted)

    compaction_percent = float(len(columnset_compacted)) / len(columnset)
    print(f"[compacted to {compaction_percent * 100:.2f}%]", end=" ")

    # test for an expected compaction rate
    assert len(columnset) > (len(columnset_compacted) * 3)


def test_to_compacted_multiple_columns_parallized(naturalearth_africa_dataframe_8):
    """uses larger amounts of data, to tests the multithreaded compacting impl"""
    columnset = ColumnSet.from_dataframe(
        naturalearth_africa_dataframe_8.loc[:, ['h3index', 'pop_est', 'gdp_md_est']]  # drop all columns except these
    )
    columnset_compacted = columnset.to_compacted()
    assert not columnset_compacted.empty
    assert len(columnset) > len(columnset_compacted)

    compaction_percent = float(len(columnset_compacted)) / len(columnset)
    print(f"[compacted to {compaction_percent * 100:.2f}%]", end=" ")

    # test for an expected compaction rate
    assert len(columnset) > (len(columnset_compacted) * 3)


def test_to_compacted_multiple_columns_raster(r_tiff_dataframe_uncompacted_8):
    columnset = ColumnSet.from_dataframe(r_tiff_dataframe_uncompacted_8)
    columnset_compacted = columnset.to_compacted()
    assert not columnset_compacted.empty
    assert len(columnset) > len(columnset_compacted)

    compaction_percent = float(len(columnset_compacted)) / len(columnset)
    print(f"[compacted to {compaction_percent * 100:.2f}%]", end=" ")

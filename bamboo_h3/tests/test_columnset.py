from bamboo_h3 import ColumnSet

# noinspection PyUnresolvedReferences
from .fixtures import naturalearth_africa_dataframe_4


def test_to_compacted_single_column(naturalearth_africa_dataframe_4):
    columnset = ColumnSet.from_dataframe(
        naturalearth_africa_dataframe_4.loc[:, ['h3index']]  # drop all columns except the h3index
    )
    columnset_compacted = columnset.to_compacted()
    assert not columnset_compacted.empty
    assert len(columnset) > len(columnset_compacted)

    compaction_percent = float(len(columnset_compacted)) / len(columnset)
    print(f"[compacted to {compaction_percent * 100:.2f}%]", end=" ")


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

from bamboo_h3 import ColumnSet

# noinspection PyUnresolvedReferences
from .fixtures import naturalearth_africa_dataframe


def test_to_compacted_single_column(naturalearth_africa_dataframe):
    columnset = ColumnSet.from_dataframe(
        naturalearth_africa_dataframe.loc[:, ['h3index']]  # drop all columns except the h3index
    )
    columnset_compacted = columnset.to_compacted()
    assert not columnset_compacted.empty
    assert len(columnset) > len(columnset_compacted)


def test_to_compacted_multiple_columns(naturalearth_africa_dataframe):
    columnset = ColumnSet.from_dataframe(
        naturalearth_africa_dataframe.loc[:, ['h3index', 'pop_est', 'gdp_md_est']]  # drop all columns except these
    )
    columnset_compacted = columnset.to_compacted()
    assert not columnset_compacted.empty
    assert len(columnset) > len(columnset_compacted)

    compaction_percent = float(len(columnset_compacted)) / len(columnset)
    # print(f"compacted to {compaction_percent * 100:.2f}%")

    # test for an expected compaction rate
    assert (len(columnset) / 10) < len(columnset_compacted)

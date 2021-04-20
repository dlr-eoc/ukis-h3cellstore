import h3.api.numpy_int as h3
import numpy as np
from bamboo_h3 import ColumnSet
from bamboo_h3.bamboo_h3 import intersect_columnset_with_indexes

# noinspection PyUnresolvedReferences
from .fixtures import naturalearth_africa_geodataframe


def test_intersect_columnset_with_indexes(naturalearth_africa_geodataframe):
    df = naturalearth_africa_geodataframe.loc[:, ["pop_est", "country_id"]]
    wkb_list = [g.wkb for g in naturalearth_africa_geodataframe.geometry]
    start_index = h3.geo_to_h3(33.90371, -0.95000, 6)

    h3indexes = h3.k_ring(start_index, k=20)

    out_df = ColumnSet(intersect_columnset_with_indexes(
        ColumnSet.from_dataframe(df).inner,
        wkb_list,
        h3indexes,
    )).to_dataframe()

    assert len(out_df) == len(h3indexes)  # mo overlap between countries, no indexes in the ocean
    assert "pop_est" in out_df
    assert "country_id" in out_df
    assert "h3index" in out_df
    assert out_df.h3index.dtype == np.uint64
    assert len(out_df.h3index.unique()) == len(h3indexes)

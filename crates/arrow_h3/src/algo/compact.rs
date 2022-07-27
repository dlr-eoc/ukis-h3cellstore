use std::borrow::Borrow;
use std::cmp::Ordering;
use std::sync::Arc;

use h3ron::collections::{CompactedCellVec, H3CellSet};
use h3ron::iter::change_resolution;
use h3ron::{H3Cell, Index};
use polars::export::arrow::array::new_empty_array;
use polars::export::arrow::datatypes::DataType;
use polars::prelude::{col, IntoLazy};
use polars_core::frame::DataFrame;
use polars_core::prelude::NamedFrom;
use polars_core::series::Series;
use tracing::{span, Level};

use crate::algo::{IterSeriesIndexes, ToIndexCollection};
use crate::{Error, H3DataFrame};

pub trait Compact {
    /// Also handles partially compacted and pre-compacted data
    fn compact(self) -> Result<Self, Error>
    where
        Self: Sized;
}

pub trait UnCompact {
    fn uncompact(self, target_resolution: u8) -> Result<Self, Error>
    where
        Self: Sized;

    /// uncompact but limit the uncompaction the given cells. all other contents
    /// will be discarded
    fn uncompact_restricted<I>(
        self,
        target_resolution: u8,
        restricted_subset: I,
    ) -> Result<Self, Error>
    where
        Self: Sized,
        I: IntoIterator,
        I::Item: Borrow<H3Cell>;
}

impl Compact for H3DataFrame {
    fn compact(self) -> Result<Self, Error> {
        let span = span!(
            Level::DEBUG,
            "Compacting H3DataFrame",
            n_rows = self.dataframe.shape().0,
            n_columns = self.dataframe.shape().1
        );
        let _enter = span.enter();

        let group_by_columns = self
            .dataframe
            .fields()
            .iter()
            .filter_map(|field| {
                if field.name() != &self.h3index_column_name {
                    Some(col(field.name()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if group_by_columns.is_empty() {
            let out_series =
                compact_cell_series(self.dataframe.column(&self.h3index_column_name)?)?;
            (DataFrame::new(vec![out_series])?, self.h3index_column_name).try_into()
        } else {
            let grouped = self
                .dataframe
                .lazy()
                .groupby(&group_by_columns)
                .agg(&[col(&self.h3index_column_name).list()])
                .collect()?;

            let mut compacted_series_vec = Vec::<Series>::with_capacity(grouped.shape().0);
            for cell_index_list in grouped
                .column(&self.h3index_column_name)?
                .list()?
                .amortized_iter()
            {
                let compacted_series = if let Some(cell_index_list) = cell_index_list {
                    compact_cell_series(cell_index_list.as_ref())?
                } else {
                    Series::try_from(("", Arc::from(new_empty_array(DataType::UInt64))))?
                };
                compacted_series_vec.push(compacted_series);
            }

            (
                grouped
                    .drop(&self.h3index_column_name)?
                    .with_column(Series::new(&self.h3index_column_name, compacted_series_vec))?
                    .explode([&self.h3index_column_name])?,
                self.h3index_column_name,
            )
                .try_into()
        }
    }
}

fn compact_cell_series(series: &Series) -> Result<Series, Error> {
    let mut ccv = CompactedCellVec::new();
    ccv.add_cells(
        series
            .iter_indexes::<H3Cell>()?
            .collect::<Result<Vec<_>, _>>()?,
        true,
    )?;
    Ok(Series::new(
        series.name(), // always keep the name of the imput series
        ccv.iter_compacted_cells()
            .map(|cell| cell.h3index() as u64)
            .collect::<Vec<_>>(),
    ))
}

const UNCOMPACT_HELPER_COL_NAME: &str = "_uncompact_helper_idx";

impl UnCompact for H3DataFrame {
    fn uncompact(self, target_resolution: u8) -> Result<Self, Error>
    where
        Self: Sized,
    {
        uncompact_h3dataframe(self, target_resolution, |_| true)
    }

    fn uncompact_restricted<I>(
        self,
        target_resolution: u8,
        restricted_subset: I,
    ) -> Result<Self, Error>
    where
        Self: Sized,
        I: IntoIterator,
        I::Item: Borrow<H3Cell>,
    {
        let subset = change_resolution(restricted_subset.into_iter(), target_resolution)
            .collect::<Result<H3CellSet, _>>()?;

        uncompact_h3dataframe(self, target_resolution, |cell| subset.contains(cell))
    }
}

fn uncompact_h3dataframe<F>(
    h3df: H3DataFrame,
    target_resolution: u8,
    cell_filter: F,
) -> Result<H3DataFrame, Error>
where
    F: Fn(&H3Cell) -> bool,
{
    let span = span!(
        Level::DEBUG,
        "Uncompacting H3DataFrame",
        n_rows = h3df.dataframe.shape().0,
        n_columns = h3df.dataframe.shape().1
    );
    let _enter = span.enter();

    // create a temporary df mapping index to uncompacted indexes to use for joining
    let mut original_index = Vec::with_capacity(h3df.dataframe.shape().0);
    let mut uncompacted_indexes = Vec::with_capacity(h3df.dataframe.shape().0);

    for unique_cell in h3df.to_index_collection::<H3CellSet, _>()? {
        match unique_cell.resolution().cmp(&target_resolution) {
            Ordering::Less => {
                // todo: Not needing to un-compact all children when a filter is specified would be an improvement,
                //     especially with large resolution differences.
                for child_cell in unique_cell
                    .get_children(target_resolution)?
                    .iter()
                    .filter(&cell_filter)
                {
                    original_index.push(unique_cell.h3index() as u64);
                    uncompacted_indexes.push(child_cell.h3index() as u64);
                }
            }
            Ordering::Equal => {
                if cell_filter(&unique_cell) {
                    original_index.push(unique_cell.h3index() as u64);
                    uncompacted_indexes.push(unique_cell.h3index() as u64);
                }
            }
            Ordering::Greater => {
                // skip smaller cells as they can not be brought up to smaller resolutions without
                // skewing data.
            }
        }
    }

    // early exit when uncompaction does not cause a change
    if original_index == uncompacted_indexes {
        return Ok(h3df);
    }

    let join_df = DataFrame::new(vec![
        Series::new(&h3df.h3index_column_name, original_index),
        Series::new(UNCOMPACT_HELPER_COL_NAME, uncompacted_indexes),
    ])?;

    let out_df = h3df
        .dataframe
        .lazy()
        .inner_join(
            join_df.lazy(),
            col(&h3df.h3index_column_name),
            col(&h3df.h3index_column_name),
        )
        .drop_columns(&[&h3df.h3index_column_name])
        .rename(&[UNCOMPACT_HELPER_COL_NAME], &[&h3df.h3index_column_name])
        .collect()?;
    (out_df, h3df.h3index_column_name).try_into()
}

#[cfg(test)]
mod tests {
    use h3ron::{H3Cell, Index};
    use itertools::Itertools;

    use crate::algo::compact::{Compact, UnCompact};
    use crate::algo::tests::make_h3_dataframe;
    use crate::algo::{ObtainH3Resolutions, ToIndexCollection};
    use crate::H3DataFrame;

    fn compact_roundtrip_dataframe_helper(value: Option<u32>) {
        let max_res = 8;
        let h3df = make_h3_dataframe(max_res, value).unwrap();
        let shape_before = h3df.dataframe.shape();
        let name_before = h3df.h3index_column_name.clone();

        let compacted = h3df.compact().unwrap();

        assert!(shape_before.0 > compacted.dataframe.shape().0);
        assert_eq!(shape_before.1, compacted.dataframe.shape().1);
        assert_eq!(name_before, compacted.h3index_column_name);

        let resolutions = compacted.h3_resolutions().unwrap();
        assert_eq!(resolutions.len(), compacted.dataframe.shape().0);
        for res in resolutions {
            assert!(res <= max_res)
        }

        let uncompacted = compacted.uncompact(max_res).unwrap();
        assert_eq!(shape_before, uncompacted.dataframe.shape());
        assert_eq!(name_before, uncompacted.h3index_column_name);

        let resolutions = uncompacted.h3_resolutions().unwrap();
        assert_eq!(resolutions.len(), uncompacted.dataframe.shape().0);
        for res in resolutions {
            assert_eq!(res, max_res);
        }
    }

    #[test]
    fn compact_roundtrip_dataframe_with_value() {
        compact_roundtrip_dataframe_helper(Some(7))
    }

    #[test]
    fn compact_roundtrip_dataframe_without_value() {
        compact_roundtrip_dataframe_helper(None)
    }

    #[test]
    fn uncompact_restricted() {
        let origin_cell = H3Cell::from_coordinate((12.0, 12.0).into(), 5).unwrap();
        let h3df =
            H3DataFrame::from_cell_iter(origin_cell.grid_disk(10).unwrap().iter(), "myindex")
                .unwrap();

        let subset_origin = origin_cell.center_child(7).unwrap();
        let subset = subset_origin
            .grid_disk(1)
            .unwrap()
            .iter()
            .sorted()
            .collect::<Vec<_>>();
        let subset_h3df = h3df
            .uncompact_restricted(subset_origin.resolution(), subset.iter())
            .unwrap();
        assert_eq!(subset_h3df.dataframe.shape().0, subset.len());

        let subset_from_subset_h3df = {
            let mut sbs: Vec<H3Cell> = subset_h3df.to_index_collection().unwrap();
            sbs.sort();
            sbs
        };
        assert_eq!(subset, subset_from_subset_h3df);
    }
}

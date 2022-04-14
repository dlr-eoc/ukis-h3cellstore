use h3ron::collections::CompactedCellVec;
use h3ron::{H3Cell, Index};
use polars::prelude::{col, IntoLazy};
use polars_core::frame::DataFrame;
use polars_core::prelude::NamedFrom;
use polars_core::series::Series;
use tracing::Level;

use crate::frame::series_iter_indexes;
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
}

impl Compact for H3DataFrame {
    fn compact(self) -> Result<Self, Error> {
        let span = tracing::span!(
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
            Ok(H3DataFrame::from_dataframe(
                DataFrame::new(vec![out_series])?,
                self.h3index_column_name,
            )?)
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
                    // todo: not sure if this is correct
                    Series::new("", Vec::<u64>::new())
                };
                compacted_series_vec.push(compacted_series);
            }

            H3DataFrame::from_dataframe(
                grouped
                    .drop(&self.h3index_column_name)?
                    .with_column(Series::new(&self.h3index_column_name, compacted_series_vec))?
                    .explode([&self.h3index_column_name])?,
                self.h3index_column_name,
            )
        }
    }
}

fn compact_cell_series(series: &Series) -> Result<Series, Error> {
    let mut ccv = CompactedCellVec::new();
    ccv.add_cells(series_iter_indexes::<H3Cell>(series)?, true)?;
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
        let span = tracing::span!(
            Level::DEBUG,
            "Uncompacting H3DataFrame",
            n_rows = self.dataframe.shape().0,
            n_columns = self.dataframe.shape().1
        );
        let _enter = span.enter();

        // create a temporary df mapping index to uncompacted indexes to use for joining
        let mut original_index = Vec::with_capacity(self.dataframe.shape().0);
        let mut uncompacted_indexes = Vec::with_capacity(self.dataframe.shape().0);

        for cell in self.iter_indexes::<H3Cell>()? {
            if cell.resolution() >= target_resolution {
                original_index.push(cell.h3index() as u64);
                uncompacted_indexes.push(cell.h3index() as u64);
            } else {
                for child_cell in cell.get_children(target_resolution)?.iter() {
                    original_index.push(cell.h3index() as u64);
                    uncompacted_indexes.push(child_cell.h3index() as u64);
                }
            }
        }

        let join_df = DataFrame::new(vec![
            Series::new(&self.h3index_column_name, original_index),
            Series::new(UNCOMPACT_HELPER_COL_NAME, uncompacted_indexes),
        ])?;

        let out_df = self
            .dataframe
            .lazy()
            .inner_join(
                join_df.lazy(),
                col(&self.h3index_column_name),
                col(&self.h3index_column_name),
            )
            .drop_columns(&[&self.h3index_column_name])
            .rename(&[UNCOMPACT_HELPER_COL_NAME], &[&self.h3index_column_name])
            .collect()?;

        (out_df, self.h3index_column_name).try_into()
    }
}

#[cfg(test)]
mod tests {
    use h3ron::H3Cell;
    use polars_core::frame::DataFrame;
    use polars_core::prelude::NamedFrom;
    use polars_core::series::Series;

    use crate::compact::{Compact, UnCompact};
    use crate::frame::to_index_series;
    use crate::{Error, H3DataFrame};

    fn make_h3_dataframe(h3_resolution: u8, value: Option<u32>) -> Result<H3DataFrame, Error> {
        let cell_h3indexes = to_index_series(
            "cell_h3index",
            H3Cell::from_coordinate((10.0, 20.0).into(), h3_resolution)?
                .grid_disk(10)?
                .iter()
                .chain(
                    H3Cell::from_coordinate((45.0, 45.0).into(), h3_resolution)?
                        .grid_disk(3)?
                        .iter(),
                ),
        );
        let count = cell_h3indexes.len();
        let mut series_vec = vec![cell_h3indexes];

        if let Some(value) = value {
            series_vec.push(Series::new(
                "value",
                (0..count).map(|_| value).collect::<Vec<_>>(),
            ));
        }
        let df = DataFrame::new(series_vec)?;
        Ok(H3DataFrame::from_dataframe(df, "cell_h3index")?)
    }

    fn compact_roundtrip_dataframe_helper(value: Option<u32>) {
        let max_res = 8;
        let h3df = make_h3_dataframe(max_res, value).unwrap();
        let shape_before = h3df.dataframe.shape();
        let name_before = h3df.h3index_column_name.clone();

        let compacted = h3df.compact().unwrap();

        assert!(shape_before.0 > compacted.dataframe.shape().0);
        assert_eq!(shape_before.1, compacted.dataframe.shape().1);
        assert_eq!(name_before, compacted.h3index_column_name);

        let resolutions = compacted.resolutions().unwrap();
        assert_eq!(resolutions.len(), compacted.dataframe.shape().0);
        for res in resolutions {
            assert!(res <= max_res)
        }

        let uncompacted = compacted.uncompact(max_res).unwrap();
        assert_eq!(shape_before, uncompacted.dataframe.shape());
        assert_eq!(name_before, uncompacted.h3index_column_name);

        let resolutions = uncompacted.resolutions().unwrap();
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
}

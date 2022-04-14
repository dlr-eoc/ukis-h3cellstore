//! based on https://github.com/nmandery/route3/blob/main/crates/s3io/src/dataframe.rs
//!
//!

use std::borrow::Borrow;
use std::fmt::{Debug, Display, Formatter};
use std::iter::FromIterator;
use std::marker::PhantomData;

use h3ron::{H3Cell, Index};
use itertools::Itertools;
use polars_core::prelude::{DataFrame, DataType, JoinType, NamedFrom, PolarsIterator, Series};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::Error;

/// wrapper around a `DataFrame` to store a bit of metainformation
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct H3DataFrame {
    /// the dataframe itself
    pub dataframe: DataFrame,

    /// name of the column containing the h3indexes.
    pub h3index_column_name: String,
}

impl Default for H3DataFrame {
    fn default() -> Self {
        Self {
            dataframe: Default::default(),
            h3index_column_name: "h3index".to_string(),
        }
    }
}

impl Debug for H3DataFrame {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.dataframe, f)
    }
}

impl Display for H3DataFrame {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.dataframe, f)
    }
}

impl H3DataFrame {
    pub fn from_dataframe<S>(dataframe: DataFrame, h3index_column_name: S) -> Result<Self, Error>
    where
        S: AsRef<str>,
    {
        tracing::info!(
            "loaded dataframe with {:?} shape, columns: {}",
            dataframe.shape(),
            dataframe.get_column_names().join(", ")
        );
        let h3index_column_name_string = h3index_column_name.as_ref().to_string();
        match dataframe.column(&h3index_column_name_string) {
            Ok(column) => {
                if column.dtype() != &DataType::UInt64 {
                    return Err(Error::DataframeInvalidH3IndexType(
                        h3index_column_name_string,
                        column.dtype().to_string(),
                    ));
                }
            }
            Err(_) => {
                return Err(Error::DataframeMissingColumn(h3index_column_name_string));
            }
        };

        Ok(H3DataFrame {
            dataframe,
            h3index_column_name: h3index_column_name_string,
        })
    }

    /// build a collection from a [`Series`] of `u64` from a [`DataFrame`] values.
    /// values will be validated and invalid values will be ignored.
    #[inline]
    pub fn index_collection_from_column<C, I>(&self, column_name: &str) -> Result<C, Error>
    where
        C: FromIterator<I>,
        I: Index,
    {
        let collection: C = if self.dataframe.is_empty() {
            std::iter::empty().collect()
        } else {
            series_iter_indexes(self.dataframe.column(column_name)?)?.collect()
        };
        Ok(collection)
    }

    /// build a collection from the `h3index_column` of this [`DataFrame`].
    /// values will be validated and invalid values will be ignored.
    #[inline]
    pub fn index_collection<C, I>(&self) -> Result<C, Error>
    where
        C: FromIterator<I>,
        I: Index,
    {
        self.index_collection_from_column(&self.h3index_column_name)
    }

    pub fn iter_indexes<I>(&self) -> Result<SeriesIndexIter<'_, I>, Error>
    where
        I: Index,
    {
        series_iter_indexes(self.dataframe.column(&self.h3index_column_name)?)
    }

    pub fn resolutions(&self) -> Result<Vec<u8>, Error> {
        // TODO: assumes cells
        Ok(self
            .iter_indexes::<H3Cell>()?
            .map(|cell| cell.resolution())
            .collect())
    }

    pub fn resolutions_series(&self) -> Result<Series, Error> {
        Ok(Series::new("resolutions", self.resolutions()?))
    }
}

/// create a `Series` from an iterator of `Index`-implementing values
#[inline]
pub fn to_index_series<I, IX>(series_name: &str, iter: I) -> Series
where
    I: IntoIterator,
    I::Item: Borrow<IX>,
    IX: Index,
{
    let u64_indexes = iter
        .into_iter()
        .map(|v| v.borrow().h3index())
        .collect::<Vec<_>>();
    Series::new(series_name, u64_indexes.as_slice())
}

pub struct SeriesIndexIter<'a, I> {
    phantom_data: PhantomData<I>,
    inner_iter: Box<dyn PolarsIterator<Item = Option<u64>> + 'a>,
}

impl<'a, I> Iterator for SeriesIndexIter<'a, I>
where
    I: Index,
{
    type Item = I;

    fn next(&mut self) -> Option<Self::Item> {
        #[allow(clippy::manual_flatten)]
        for item in &mut self.inner_iter {
            if let Some(h3index) = item {
                let index = I::from_h3index(h3index);
                if index.is_valid() {
                    return Some(index);
                }
                // simply ignore invalid h3indexes for now
            }
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner_iter.size_hint()
    }
}

/// build a `Iterator` of [`Index`] values from a [`Series`] of `u64` values.
///
/// values will be validated and invalid values will be ignored.
pub fn series_iter_indexes<I>(series: &Series) -> Result<SeriesIndexIter<I>, Error>
where
    I: Index,
{
    let inner = series.u64()?.into_iter();
    Ok(SeriesIndexIter {
        phantom_data: PhantomData::<I>::default(),
        inner_iter: inner,
    })
}

/// add a prefix to all columns in the dataframe
pub fn prefix_column_names(dataframe: &mut DataFrame, prefix: &str) -> Result<(), Error> {
    let col_names = dataframe
        .get_column_names()
        .iter()
        .map(|cn| cn.to_string())
        .sorted_by_key(|cn| cn.len()) // sort by length descending to avoid duplicated column names -> error
        .rev()
        .collect::<Vec<_>>();
    for col_name in col_names {
        dataframe.rename(&col_name, &format!("{}{}", prefix, col_name))?;
    }
    Ok(())
}

/// inner-join a [`H3DataFrame`] to the given `dataframe` using the specified `prefix`
pub fn inner_join_h3dataframe(
    dataframe: &mut DataFrame,
    dataframe_h3index_column: &str,
    mut h3dataframe: H3DataFrame,
    prefix: &str,
) -> Result<(), Error> {
    // add prefix for origin columns
    prefix_column_names(&mut h3dataframe.dataframe, prefix)?;

    *dataframe = dataframe.join(
        &h3dataframe.dataframe,
        [dataframe_h3index_column],
        [format!("{}{}", prefix, h3dataframe.h3index_column_name).as_str()],
        JoinType::Inner,
        None,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use h3ron::{H3Cell, Index};
    use polars_core::prelude::*;

    use super::{series_iter_indexes, to_index_series};

    #[test]
    fn test_to_index_series() {
        let idx = H3Cell::new(0x89283080ddbffff_u64);
        let series = to_index_series("cells", &idx.grid_disk(1).unwrap());
        assert_eq!(series.name(), "cells");
        assert_eq!(series.len(), 7);
    }

    #[test]
    fn test_series_index_iter() {
        let series = Series::new("cells", &[0x89283080ddbffff_u64]);
        let cells = series_iter_indexes(&series)
            .unwrap()
            .collect::<Vec<H3Cell>>();
        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0], H3Cell::new(0x89283080ddbffff_u64));
    }
}

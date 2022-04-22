use std::borrow::Borrow;
use std::marker::PhantomData;

use h3ron::Index;
use polars_core::prelude::{NamedFrom, PolarsIterator, Series};

use crate::Error;

/// create a `Series` from an iterator of `Index`-implementing values
#[inline]
pub fn to_index_series<I, IX>(series_name: &str, iter: I) -> Series
where
    I: IntoIterator,
    I::Item: Borrow<IX>,
    IX: Index,
{
    Series::new(
        series_name,
        iter.into_iter()
            .map(|v| v.borrow().h3index())
            .collect::<Vec<_>>(),
    )
}

pub struct SeriesIndexSkipInvalidIter<'a, I> {
    phantom_data: PhantomData<I>,
    inner_iter: Box<dyn PolarsIterator<Item = Option<u64>> + 'a>,
}

impl<'a, I> Iterator for SeriesIndexSkipInvalidIter<'a, I>
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
pub fn series_iter_indexes_skip_invalid<I>(
    series: &Series,
) -> Result<SeriesIndexSkipInvalidIter<I>, Error>
where
    I: Index,
{
    let inner = series.u64()?.into_iter();
    Ok(SeriesIndexSkipInvalidIter {
        phantom_data: PhantomData::<I>::default(),
        inner_iter: inner,
    })
}

pub struct SeriesIndexIter<'a, I> {
    phantom_data: PhantomData<I>,
    inner_iter: Box<dyn PolarsIterator<Item = Option<u64>> + 'a>,
}

impl<'a, I> Iterator for SeriesIndexIter<'a, I>
where
    I: Index + TryFrom<u64, Error = h3ron::Error>,
{
    type Item = Result<I, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        #[allow(clippy::manual_flatten)]
        match &mut self.inner_iter.next() {
            None => None,
            Some(index_opt) => match index_opt {
                Some(index) => Some(I::try_from(*index).map_err(Error::from)),
                None => Some(Err(Error::MissingIndexValue)),
            },
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner_iter.size_hint()
    }
}

/// build a `Iterator` of [`Index`] values from a [`Series`] of `u64` values.
///
/// Fails on missing and invalid values
pub fn series_iter_indexes<I>(series: &Series) -> Result<SeriesIndexIter<I>, Error>
where
    I: Index + TryFrom<u64, Error = h3ron::Error>,
{
    let inner = series.u64()?.into_iter();
    Ok(SeriesIndexIter {
        phantom_data: PhantomData::<I>::default(),
        inner_iter: inner,
    })
}

#[cfg(test)]
mod tests {
    use h3ron::{H3Cell, Index};
    use polars_core::prelude::{NamedFrom, Series};

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
            .collect::<Result<Vec<H3Cell>, _>>()
            .unwrap();
        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0], H3Cell::new(0x89283080ddbffff_u64));
    }
}

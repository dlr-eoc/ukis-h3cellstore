use h3ron::Index;
use polars_core::prelude::Series;

use crate::series::{
    series_iter_indexes, series_iter_indexes_skip_invalid, SeriesIndexIter,
    SeriesIndexSkipInvalidIter,
};
use crate::{Error, H3DataFrame};

/// iterate over parts of container yielding chunks of max. `max_num_rows` rows
pub trait IterRowCountLimited<'a> {
    type Iter;

    /// iterate over parts of container yielding chunks of max. `max_num_rows` rows
    fn iter_row_count_limited(&'a self, max_num_rows: usize) -> Result<Self::Iter, Error>;
}

impl<'a> IterRowCountLimited<'a> for H3DataFrame {
    type Iter = H3DataFrameRCLIter<'a>;

    fn iter_row_count_limited(&'a self, max_num_rows: usize) -> Result<Self::Iter, Error> {
        let num_rows_in_df = self.dataframe.shape().0;
        Ok(H3DataFrameRCLIter {
            h3df: self,
            num_rows_in_df,
            max_num_rows,
            current_offset: 0,
        })
    }
}

pub struct H3DataFrameRCLIter<'a> {
    h3df: &'a H3DataFrame,
    num_rows_in_df: usize,
    max_num_rows: usize,
    current_offset: i64,
}

impl<'a> Iterator for H3DataFrameRCLIter<'a> {
    type Item = H3DataFrame;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_offset as usize >= self.num_rows_in_df {
            None
        } else {
            let iter_h3df = H3DataFrame {
                dataframe: self
                    .h3df
                    .dataframe
                    .slice(self.current_offset, self.max_num_rows),
                h3index_column_name: self.h3df.h3index_column_name.clone(),
            };
            self.current_offset += self.max_num_rows as i64;
            Some(iter_h3df)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            (self
                .num_rows_in_df
                .saturating_sub(self.current_offset as usize * self.max_num_rows)
                as f64
                / self.max_num_rows as f64)
                .ceil() as usize,
            None,
        )
    }
}

pub trait IterSeriesIndexes {
    fn iter_indexes<I>(&self) -> Result<SeriesIndexIter<'_, I>, Error>
    where
        I: Index + TryFrom<u64, Error = h3ron::Error>;
}

impl IterSeriesIndexes for Series {
    fn iter_indexes<I>(&self) -> Result<SeriesIndexIter<'_, I>, Error>
    where
        I: Index + TryFrom<u64, Error = h3ron::Error>,
    {
        series_iter_indexes(self)
    }
}

impl IterSeriesIndexes for H3DataFrame {
    fn iter_indexes<I>(&self) -> Result<SeriesIndexIter<'_, I>, Error>
    where
        I: Index + TryFrom<u64, Error = h3ron::Error>,
    {
        self.index_series()?.iter_indexes()
    }
}

pub trait IterSeriesIndexesSkipInvalid {
    fn iter_indexes_skip_invalid<I>(&self) -> Result<SeriesIndexSkipInvalidIter<'_, I>, Error>
    where
        I: Index;
}

impl IterSeriesIndexesSkipInvalid for Series {
    fn iter_indexes_skip_invalid<I>(&self) -> Result<SeriesIndexSkipInvalidIter<'_, I>, Error>
    where
        I: Index,
    {
        series_iter_indexes_skip_invalid(self)
    }
}

impl IterSeriesIndexesSkipInvalid for H3DataFrame {
    fn iter_indexes_skip_invalid<I>(&self) -> Result<SeriesIndexSkipInvalidIter<'_, I>, Error>
    where
        I: Index,
    {
        self.index_series()?.iter_indexes_skip_invalid()
    }
}

pub trait ToIndexCollection {
    /// build a collection from the contained Indexes.
    fn to_index_collection<C, I>(&self) -> Result<C, Error>
    where
        C: FromIterator<I>,
        I: Index + TryFrom<u64, Error = h3ron::Error>;
}

impl ToIndexCollection for Series {
    fn to_index_collection<C, I>(&self) -> Result<C, Error>
    where
        C: FromIterator<I>,
        I: Index + TryFrom<u64, Error = h3ron::Error>,
    {
        self.iter_indexes()?.collect::<Result<C, _>>()
    }
}

impl ToIndexCollection for H3DataFrame {
    fn to_index_collection<C, I>(&self) -> Result<C, Error>
    where
        C: FromIterator<I>,
        I: Index + TryFrom<u64, Error = h3ron::Error>,
    {
        if self.dataframe.is_empty() {
            Ok(std::iter::empty().collect())
        } else {
            self.dataframe
                .column(&self.h3index_column_name)?
                .to_index_collection()
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::algo::tests::make_h3_dataframe;
    use crate::algo::IterRowCountLimited;

    #[test]
    fn iter_row_count_limited() {
        let h3df = make_h3_dataframe(6, Some(4)).unwrap();

        let max_num_rows = 10;
        {
            let iter = h3df.iter_row_count_limited(max_num_rows).unwrap();
            let num_expected = iter.size_hint().0;
            assert!(num_expected > 2);
            assert_eq!(
                h3df.iter_row_count_limited(max_num_rows).unwrap().count(),
                num_expected
            );
        }
        assert!(h3df
            .iter_row_count_limited(max_num_rows)
            .unwrap()
            .all(|part| part.dataframe.shape().0 <= max_num_rows));
        assert!(h3df
            .iter_row_count_limited(max_num_rows)
            .unwrap()
            .all(|part| part.h3index_column_name == h3df.h3index_column_name));
    }
}

//! based on https://github.com/nmandery/route3/blob/main/crates/s3io/src/dataframe.rs
//!
//!

use std::borrow::Borrow;
use std::fmt::{Debug, Display, Formatter};
use std::iter::FromIterator;

use h3ron::{H3Cell, Index};
use itertools::Itertools;
use polars_core::prelude::{DataFrame, DataType, JoinType, Series};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::algo::iter::ToIndexCollection;
use crate::series::to_index_series;
use crate::Error;

/// wrapper around a `DataFrame` to store a bit of metainformation
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
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
        tracing::debug!(
            "loaded dataframe with {:?} shape, columns: {}",
            dataframe.shape(),
            dataframe.get_column_names().join(", ")
        );
        let h3df = H3DataFrame {
            dataframe,
            h3index_column_name: h3index_column_name.as_ref().to_string(),
        };
        h3df.validate()?;
        Ok(h3df)
    }

    pub fn from_cell_iter<I, S>(cells_iter: I, h3index_column_name: S) -> Result<Self, Error>
    where
        S: AsRef<str>,
        I: IntoIterator,
        I::Item: Borrow<H3Cell>,
    {
        Ok(Self {
            dataframe: DataFrame::new(vec![to_index_series(
                h3index_column_name.as_ref(),
                cells_iter,
            )])?,
            h3index_column_name: h3index_column_name.as_ref().to_string(),
        })
    }

    pub fn validate(&self) -> Result<(), Error> {
        match self.index_series() {
            Ok(column) => {
                if column.dtype() != &DataType::UInt64 {
                    return Err(Error::DataframeInvalidH3IndexType(
                        self.h3index_column_name.clone(),
                        column.dtype().to_string(),
                    ));
                }
            }
            Err(_) => {
                return Err(Error::DataframeMissingColumn(
                    self.h3index_column_name.clone(),
                ));
            }
        };
        Ok(())
    }
    /// build a collection from a [`Series`] of `u64` from a [`DataFrame`] values.
    #[inline]
    pub fn index_collection_from_column<C, I>(&self, column_name: &str) -> Result<C, Error>
    where
        C: FromIterator<I>,
        I: Index + TryFrom<u64, Error = h3ron::Error>,
    {
        if self.dataframe.is_empty() {
            Ok(std::iter::empty().collect())
        } else {
            self.dataframe.column(column_name)?.to_index_collection()
        }
    }

    /// reference to the series containing the indexes
    #[inline]
    pub fn index_series(&self) -> Result<&Series, Error> {
        Ok(self.dataframe.column(&self.h3index_column_name)?)
    }
}

impl<S> TryFrom<(DataFrame, S)> for H3DataFrame
where
    S: AsRef<str>,
{
    type Error = Error;

    fn try_from(value: (DataFrame, S)) -> Result<Self, Self::Error> {
        H3DataFrame::from_dataframe(value.0, value.1)
    }
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

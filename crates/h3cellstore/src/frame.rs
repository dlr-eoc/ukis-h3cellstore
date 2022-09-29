use crate::Error;
use h3ron::H3Cell;
use h3ron_polars::{AsH3IndexChunked, IndexValue};
use polars::prelude::{DataFrame, Series};
use std::fmt::{Debug, Display, Formatter};

use clickhouse_arrow_grpc::export::arrow2::datatypes::DataType;
#[cfg(feature = "use-serde")]
use serde::{Deserialize, Serialize};

/// wrapper around a `DataFrame` to store a bit of metainformation
#[cfg_attr(feature = "use-serde", derive(Serialize, Deserialize))]
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
    pub fn to_index_collection_from_column<C, I>(&self, column_name: &str) -> Result<C, Error>
    where
        C: FromIterator<I>,
        I: IndexValue + TryFrom<u64, Error = h3ron::Error>,
    {
        let ic = self
            .dataframe
            .column(column_name)?
            .u64()?
            .h3indexchunked::<I>();

        Ok(ic
            .iter_indexes_validated()
            .flatten()
            .collect::<Result<C, _>>()?)
    }

    #[inline]
    pub fn to_index_collection<C>(&self) -> Result<C, Error>
    where
        C: FromIterator<H3Cell>,
    {
        let ic = self.index_series()?.u64()?.h3indexchunked::<H3Cell>();
        Ok(ic
            .iter_indexes_validated()
            .flatten()
            .collect::<Result<C, _>>()?)
    }

    /// reference to the series containing the indexes
    #[inline]
    pub fn index_series(&self) -> Result<&Series, Error> {
        Ok(self.dataframe.column(&self.h3index_column_name)?)
    }
}

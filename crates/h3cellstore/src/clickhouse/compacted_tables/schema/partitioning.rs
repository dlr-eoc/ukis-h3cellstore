use std::any::type_name;

#[cfg(feature = "use_serde")]
use serde::{Deserialize, Serialize};

use crate::clickhouse::compacted_tables::schema::ValidateSchema;
use crate::Error;

#[derive(Debug, PartialEq, Eq, Clone)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub enum TemporalResolution {
    Second,
    Day,
}

impl Default for TemporalResolution {
    fn default() -> Self {
        TemporalResolution::Second
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub enum TemporalPartitioning {
    /// Monthly/multi-month partitions
    Months(u8),

    /// Partitions of `n` years
    Years(u8),
}

impl Default for TemporalPartitioning {
    fn default() -> Self {
        Self::Years(1)
    }
}

impl ValidateSchema for TemporalPartitioning {
    fn validate(&self) -> Result<(), Error> {
        match self {
            Self::Months(num_months) => {
                if *num_months == 0 {
                    return Err(Error::SchemaValidationError(
                        type_name::<Self>(),
                        "number of months must be > 0".to_string(),
                    ));
                }
            }
            Self::Years(num_years) => {
                if *num_years == 0 {
                    return Err(Error::SchemaValidationError(
                        type_name::<Self>(),
                        "number of years must be > 0".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

impl TemporalPartitioning {
    pub(crate) fn sql_expression<S>(&self, column_name: S) -> String
    where
        S: AsRef<str>,
    {
        match self {
            Self::Months(num_months) => {
                format!(
                    "toString(ceil(((toYear({}) * 100) + (100 * (toMonth({}) - 1) / 12)) / {}))",
                    column_name.as_ref(),
                    column_name.as_ref(),
                    *num_months
                )
            }
            Self::Years(num_years) => {
                if *num_years == 1 {
                    format!("toString(toYear({}))", column_name.as_ref())
                } else {
                    // reshaping the year according to num_years
                    //
                    // With num_years == 3, value '2019' will contain the years 2019, 2020 and 2021.
                    format!(
                        "toString(floor(toYear({})/{})*{})",
                        column_name.as_ref(),
                        num_years,
                        num_years
                    )
                }
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub enum H3Partitioning {
    /// partition by basecell
    BaseCell,

    /// partition by the cells of a lower resolution.
    ///
    /// The parent_resolution is the given number of levels less than the h3 resolution of the index
    /// the partitioning is applied to.
    LowerResolution(u8),
}

impl Default for H3Partitioning {
    fn default() -> Self {
        Self::BaseCell
    }
}

impl H3Partitioning {
    pub(crate) fn sql_expression<S>(&self, column_name: S) -> String
    where
        S: AsRef<str>,
    {
        match self {
            Self::BaseCell => format!("h3GetBaseCell({})", column_name.as_ref()),
            Self::LowerResolution(resolution_difference) => format!(
                "h3ToParent({}, cast(max2(h3GetResolution({}) - {}, 0) as UInt8))",
                column_name.as_ref(),
                column_name.as_ref(),
                resolution_difference
            ),
        }
    }
}

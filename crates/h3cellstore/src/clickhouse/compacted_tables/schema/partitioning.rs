use std::any::type_name;

#[cfg(feature = "use_serde")]
use serde::{Deserialize, Serialize};

use crate::clickhouse::compacted_tables::schema::ValidateSchema;
use crate::Error;

#[derive(Debug, PartialEq, Clone)]
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

#[derive(Debug, PartialEq, Clone)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub enum TemporalPartitioning {
    Month,
    Years(u8),
}

impl Default for TemporalPartitioning {
    fn default() -> Self {
        TemporalPartitioning::Month
    }
}

impl ValidateSchema for TemporalPartitioning {
    fn validate(&self) -> Result<(), Error> {
        if let TemporalPartitioning::Years(num_years) = self {
            if *num_years == 0 {
                return Err(Error::SchemaValidationError(
                    type_name::<Self>(),
                    "number of years must be > 0".to_string(),
                ));
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
            Self::Month => {
                format!("toString(toMonth({}))", column_name.as_ref())
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

#[derive(Debug, PartialEq, Clone)]
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

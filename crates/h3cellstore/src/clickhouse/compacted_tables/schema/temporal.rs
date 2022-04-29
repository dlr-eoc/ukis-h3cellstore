use std::any::type_name;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::clickhouse::compacted_tables::schema::ValidateSchema;
use crate::Error;

#[derive(Debug, PartialEq, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
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
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
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

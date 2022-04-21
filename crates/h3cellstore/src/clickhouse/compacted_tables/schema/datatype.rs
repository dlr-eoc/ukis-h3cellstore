use crate::Named;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// supported subset of the clickhouse datatypes.
///
/// https://clickhouse.com/docs/en/interfaces/formats/#data_types-matching-arrow
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum ClickhouseDataType {
    UInt8,
    Int8,
    UInt16,
    Int16,
    UInt32,
    Int32,
    UInt64,
    Int64,
    Float32,
    Float64,
    Date,
    DateTime,
    DateTime64,
    String,
}

impl ClickhouseDataType {
    pub fn is_temporal(&self) -> bool {
        matches!(self, Self::Date | Self::DateTime | Self::DateTime64)
    }

    pub fn is_signed_integer(&self) -> bool {
        matches!(self, Self::Int8 | Self::Int16 | Self::Int32 | Self::Int64)
    }

    pub fn is_unsigned_integer(&self) -> bool {
        matches!(
            self,
            Self::UInt8 | Self::UInt16 | Self::UInt32 | Self::UInt64
        )
    }

    pub fn is_float(&self) -> bool {
        matches!(self, Self::Float32 | Self::Float64)
    }

    pub fn is_number(&self) -> bool {
        self.is_signed_integer() || self.is_unsigned_integer() || self.is_float()
    }

    pub fn sql_type_name(&self) -> &'static str {
        match self {
            ClickhouseDataType::UInt8 => "UInt8",
            ClickhouseDataType::Int8 => "Int8",
            ClickhouseDataType::UInt16 => "UInt16",
            ClickhouseDataType::Int16 => "Int16",
            ClickhouseDataType::UInt32 => "UInt32",
            ClickhouseDataType::Int32 => "Int32",
            ClickhouseDataType::UInt64 => "UInt64",
            ClickhouseDataType::Int64 => "Int64",
            ClickhouseDataType::Float32 => "Float32",
            ClickhouseDataType::Float64 => "Float64",
            ClickhouseDataType::Date => "Date",
            ClickhouseDataType::DateTime => "DateTime",
            ClickhouseDataType::DateTime64 => "DateTime64",
            ClickhouseDataType::String => "String",
        }
    }
}

impl Named for ClickhouseDataType {
    fn name(&self) -> &'static str {
        self.sql_type_name()
    }
}

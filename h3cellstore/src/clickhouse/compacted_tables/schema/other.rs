use crate::clickhouse::compacted_tables::schema::ValidateSchema;
use crate::Error;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::any::type_name;

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, PartialEq, Clone)]
pub enum TableEngine {
    ReplacingMergeTree,
    SummingMergeTree(Vec<String>),
    AggregatingMergeTree,
}

impl Default for TableEngine {
    fn default() -> Self {
        TableEngine::ReplacingMergeTree
    }
}

#[derive(Debug, PartialEq, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[allow(clippy::upper_case_acronyms)]
pub enum CompressionMethod {
    LZ4HC(u8),
    ZSTD(u8),
    Delta(u8),
    DoubleDelta,
    Gorilla,
    T64,
}

impl ValidateSchema for CompressionMethod {
    fn validate(&self) -> Result<(), Error> {
        // validate compression levels
        // https://clickhouse.tech/docs/en/sql-reference/statements/create/table/#create-query-general-purpose-codecs
        match self {
            Self::ZSTD(level) => {
                if !(1u8..=22u8).contains(level) {
                    return Err(compression_level_out_of_range(type_name::<Self>()));
                }
                Ok(())
            }
            Self::LZ4HC(level) => {
                if !(1u8..=9u8).contains(level) {
                    return Err(compression_level_out_of_range(type_name::<Self>()));
                }
                Ok(())
            }
            Self::Delta(delta_bytes) => {
                if ![1, 2, 4, 8].contains(delta_bytes) {
                    return Err(Error::SchemaValidationError(
                        "Delta compression",
                        format!("Unsupported value for delta_bytes: {}", delta_bytes),
                    ));
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

fn compression_level_out_of_range(location: &'static str) -> Error {
    Error::SchemaValidationError(location, "compression level out of range".to_string())
}

impl Default for CompressionMethod {
    fn default() -> Self {
        Self::ZSTD(6)
    }
}

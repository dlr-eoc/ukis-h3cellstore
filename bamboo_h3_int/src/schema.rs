use crate::error::Error;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::any::type_name;

// templating: https://github.com/djc/askama

pub trait ValidateSchema {
    fn validate(&self) -> Result<(), Error>;
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Schema {
    CompactedTable(CompactedTableSchema),
}

impl Schema {
    fn to_json_string(&self) -> Result<String, Error> {
        self.validate()?;
        serde_json::to_string_pretty(self).map_err(|e| e.into())
    }
}

impl ValidateSchema for Schema {
    fn validate(&self) -> Result<(), Error> {
        match self {
            Self::CompactedTable(ct) => ct.validate(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct CompactedTableSchema {
    pub name: String,
    pub compression_method: TableCompressionMethod,
    pub temporal_resolution: TemporalResolution,
}

impl ValidateSchema for CompactedTableSchema {
    fn validate(&self) -> Result<(), Error> {
        validate_table_name(type_name::<Self>(), &self.name)?;
        self.compression_method.validate()
    }
}

lazy_static! {
    static ref RE_VALID_NAME: Regex = Regex::new(r"^[a-zA-Z].[_a-zA-Z_0-9]+$").unwrap();
}

fn validate_table_name(location: &'static str, name: &str) -> Result<(), Error> {
    if RE_VALID_NAME.is_match(name) {
        Ok(())
    } else {
        Err(Error::SchemaValidationError(
            location,
            format!("invalid table name: \"{}\"", name),
        ))
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum TableCompressionMethod {
    LZ4HC(u8),
    ZSTD(u8),
}

impl ValidateSchema for TableCompressionMethod {
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
        }
    }
}

fn compression_level_out_of_range(location: &'static str) -> Error {
    Error::SchemaValidationError(location, "compression level out of range".to_string())
}

impl Default for TableCompressionMethod {
    fn default() -> Self {
        Self::ZSTD(6)
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TemporalResolution {
    Second,
    Day,
}

impl Default for TemporalResolution {
    fn default() -> Self {
        TemporalResolution::Second
    }
}

#[cfg(test)]
mod tests {
    use super::validate_table_name;
    use crate::schema::{Schema, CompactedTableSchema};

    #[test]
    fn test_validate_table_name() {
        assert!(validate_table_name("unittest", "").is_err());
        assert!(validate_table_name("unittest", " test").is_err());
        assert!(validate_table_name("unittest", "4test").is_err());
        assert!(validate_table_name("unittest", "something").is_ok());
        assert!(validate_table_name("unittest", "some_thing").is_ok());
    }

    #[test]
    fn schema_to_toml() {
        let s = Schema::CompactedTable(CompactedTableSchema {
            name: "my_little_table".to_string(),
            compression_method: Default::default(),
            temporal_resolution: Default::default()
        });
        println!("{}", s.to_json_string().unwrap());
    }
}

use std::any::type_name;
use std::collections::HashMap;

use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::COL_NAME_H3INDEX;
use crate::colvec::Datatype;
use crate::common::Named;
use crate::error::Error;

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
    pub columns: HashMap<String, ColumnDefinition>,
}

impl ValidateSchema for CompactedTableSchema {
    fn validate(&self) -> Result<(), Error> {
        validate_table_name(type_name::<Self>(), &self.name)?;
        self.compression_method.validate()?;

        // a h3index column must exist
        match self.columns.get(COL_NAME_H3INDEX) {
            Some(h3index_column) => {
                if let ColumnDefinition::Simple(simple_column) = h3index_column {
                    if simple_column.datatype != Datatype::U64 {
                        return Err(Error::SchemaValidationError(
                            type_name::<Self>(),
                            format!(
                                "mandatory column {} must be typed as {}",
                                COL_NAME_H3INDEX,
                                Datatype::U64
                            ),
                        ));
                    }
                } else {
                    return Err(Error::SchemaValidationError(
                        type_name::<Self>(),
                        format!(
                            "mandatory column {} is must be a simple column",
                            COL_NAME_H3INDEX
                        ),
                    ));
                }
            }
            None => {
                return Err(Error::SchemaValidationError(
                    type_name::<Self>(),
                    format!("mandatory column {} is missing", COL_NAME_H3INDEX),
                ))
            }
        }
        Ok(())
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

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AggregationMethod {
    RelativeToArea,
    Sum,
    Max,
    Min,
    Average,
}

impl AggregationMethod {
    pub fn is_applicable_to_datatype(&self, datatype: &Datatype) -> bool {
        match self {
            Self::RelativeToArea => !(datatype.is_nullable() || datatype.is_temporal()),
            Self::Sum => !(datatype.is_nullable() || datatype.is_temporal()),
            Self::Max => !datatype.is_nullable(),
            Self::Min => !datatype.is_nullable(),
            Self::Average => !(datatype.is_nullable() || datatype.is_temporal()),
        }
    }
}

impl Named for AggregationMethod {
    fn name(&self) -> &'static str {
        match self {
            Self::RelativeToArea => "relativetoarea",
            Self::Max => "max",
            Self::Min => "min",
            Self::Sum => "sum",
            Self::Average => "average",
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ColumnDefinition {
    /// a simple column which just stores data.
    /// The data will not get modified when the values get aggregated to coarser resolutions.
    Simple(SimpleColumn),

    /// data stored in this column will be aggregated using the specified aggregation
    /// method when the coarser resolutions are generated
    WithAggregation(SimpleColumn, AggregationMethod),
}

impl ValidateSchema for ColumnDefinition {
    fn validate(&self) -> Result<(), Error> {
        match self {
            Self::WithAggregation(simple_column, aggregation_method) => {
                if !(aggregation_method.is_applicable_to_datatype(&simple_column.datatype)) {
                    return Err(Error::SchemaValidationError(
                        type_name::<Self>(),
                        format!(
                            "aggregation {} can not be applied to datatype {}",
                            aggregation_method.name(),
                            simple_column.datatype.name()
                        ),
                    ));
                }
            }
            _ => (),
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct SimpleColumn {
    datatype: Datatype,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::clickhouse::schema::{AggregationMethod, ColumnDefinition, CompactedTableSchema, Schema, SimpleColumn, ValidateSchema};
    use crate::COL_NAME_H3INDEX;
    use crate::colvec::Datatype;

    use super::validate_table_name;

    #[test]
    fn test_validate_table_name() {
        assert!(validate_table_name("unittest", "").is_err());
        assert!(validate_table_name("unittest", " test").is_err());
        assert!(validate_table_name("unittest", "4test").is_err());
        assert!(validate_table_name("unittest", "something").is_ok());
        assert!(validate_table_name("unittest", "some_thing").is_ok());
    }

    #[test]
    fn schema_to_json() {
        let s = Schema::CompactedTable(CompactedTableSchema {
            name: "my_little_table".to_string(),
            compression_method: Default::default(),
            temporal_resolution: Default::default(),
            columns: {
                let mut c = HashMap::new();
                c.insert(
                    COL_NAME_H3INDEX.to_string(),
                    ColumnDefinition::Simple(SimpleColumn {
                        datatype: Datatype::U64,
                    }),
                );
                c.insert(
                    "elephant_density".to_string(),
                    ColumnDefinition::WithAggregation(
                        SimpleColumn {
                            datatype: Datatype::F32,
                        },
                        AggregationMethod::Average,
                    ),
                );
                c
            },
        });
        s.validate().unwrap();
        println!("{}", s.to_json_string().unwrap());
    }
}

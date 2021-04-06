
use std::any::type_name;

use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::columnset::Datatype;
use crate::common::Named;
use crate::error::Error;
use clickhouse_rs::types::{DateTimeType, SqlType};
use crate::clickhouse::schema::compacted_tables::CompactedTableSchema;

pub mod compacted_tables;

pub trait ValidateSchema {
    fn validate(&self) -> Result<(), Error>;
}

pub trait CreateSchema {
    /// generate the SQL statements to create the schema
    fn create_statements(&self) -> Result<Vec<String>, Error>;
}


#[derive(Serialize, Deserialize, Debug)]
pub enum Schema {
    CompactedTable(CompactedTableSchema),
}

impl Schema {
    pub fn to_json_string(&self) -> Result<String, Error> {
        self.validate()?;
        serde_json::to_string_pretty(self).map_err(|e| e.into())
    }

    pub fn from_json_string(instr: &str) -> Result<Self, Error> {
        let schema: Schema = serde_json::from_str(instr)?;
        schema.validate()?;
        Ok(schema)
    }
}

impl ValidateSchema for Schema {
    fn validate(&self) -> Result<(), Error> {
        match self {
            Self::CompactedTable(ct) => ct.validate(),
        }
    }
}

impl CreateSchema for Schema {
    fn create_statements(&self) -> Result<Vec<String>, Error> {
        match self {
            Schema::CompactedTable(ct) => ct.create_statements()
        }
    }
}


lazy_static! {
    // validation does not include reserved SQL keywords, but Clickhouse will fail happily when
    // encountering them as a table name anyways.
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

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
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

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[allow(clippy::upper_case_acronyms)]
pub enum CompressionMethod {
    LZ4HC(u8),
    ZSTD(u8),
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

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum TemporalResolution {
    Second,
    Day,
}

impl Default for TemporalResolution {
    fn default() -> Self {
        TemporalResolution::Second
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum TemporalPartitioning {
    Year,
    Month,
}

impl Default for TemporalPartitioning {
    fn default() -> Self {
        TemporalPartitioning::Month
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum AggregationMethod {
    RelativeToCellArea,
    Sum,
    Max,
    Min,
    Average,
    // TODO: aggragation method to generate parent resolution for other h3index column
}

impl AggregationMethod {
    pub fn is_applicable_to_datatype(&self, datatype: &Datatype) -> bool {
        match self {
            Self::RelativeToCellArea => !(datatype.is_nullable() || datatype.is_temporal()),
            Self::Sum => !(datatype.is_nullable() || datatype.is_temporal()),
            Self::Max => !datatype.is_nullable(),
            Self::Min => !datatype.is_nullable(),
            Self::Average => !datatype.is_nullable(),
        }
    }
}

impl Named for AggregationMethod {
    fn name(&self) -> &'static str {
        match self {
            Self::RelativeToCellArea => "relativetocellarea",
            Self::Max => "max",
            Self::Min => "min",
            Self::Sum => "sum",
            Self::Average => "average",
        }
    }
}

trait GetSqlType {
    fn sqltype(&self) -> SqlType;
}

impl GetSqlType for Datatype {
    fn sqltype(&self) -> SqlType {
        match self {
            Datatype::U8 => SqlType::UInt8,
            Datatype::U8N => SqlType::Nullable(&SqlType::UInt8),
            Datatype::I8 => SqlType::Int8,
            Datatype::I8N => SqlType::Nullable(&SqlType::Int8),
            Datatype::U16 => SqlType::UInt16,
            Datatype::U16N => SqlType::Nullable(&SqlType::UInt16),
            Datatype::I16 => SqlType::Int16,
            Datatype::I16N => SqlType::Nullable(&SqlType::Int16),
            Datatype::U32 => SqlType::UInt32,
            Datatype::U32N => SqlType::Nullable(&SqlType::UInt32),
            Datatype::I32 => SqlType::Int32,
            Datatype::I32N => SqlType::Nullable(&SqlType::Int32),
            Datatype::U64 => SqlType::UInt64,
            Datatype::U64N => SqlType::Nullable(&SqlType::UInt64),
            Datatype::I64 => SqlType::Int64,
            Datatype::I64N => SqlType::Nullable(&SqlType::Int64),
            Datatype::F32 => SqlType::Float32,
            Datatype::F32N => SqlType::Nullable(&SqlType::Float32),
            Datatype::F64 => SqlType::Float64,
            Datatype::F64N => SqlType::Nullable(&SqlType::Float64),
            Datatype::Date => SqlType::Date,
            Datatype::DateN => SqlType::Nullable(&SqlType::Date),
            Datatype::DateTime => SqlType::DateTime(DateTimeType::Chrono),
            Datatype::DateTimeN => SqlType::Nullable(&SqlType::DateTime(DateTimeType::Chrono)),
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum ColumnDefinition {
    /// a simple column which just stores data.
    /// The data will not get modified when the values get aggregated to coarser resolutions.
    Simple(SimpleColumn),

    /// a column storing an h3index
    /// h3 indexes will always be brought the resolution of the coarser table when generating parent
    /// resolutions
    H3Index,

    /// data stored in this column will be aggregated using the specified aggregation
    /// method when the coarser resolutions are generated
    ///
    /// Aggregation only happens **within** the batch written to
    /// the tables.
    WithAggregation(SimpleColumn, AggregationMethod),
}

impl ColumnDefinition {
    pub fn datatype(&self) -> Datatype {
        match self {
            Self::H3Index => Datatype::U64,
            Self::Simple(sc) => sc.datatype.clone(),
            Self::WithAggregation(sc, _) => sc.datatype.clone(),
        }
    }

    /// position in the sorting key (`ORDER BY`) in MergeTree tables
    /// which can be unterstood as a form of a primary key. Please consult
    /// https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/mergetree/
    /// for more
    pub fn order_key_position(&self) -> Option<u8> {
        match self {
            Self::H3Index => Some(0),
            Self::Simple(sc) => sc.order_key_position,
            Self::WithAggregation(sc, _) => sc.order_key_position,
        }
    }
}

impl ValidateSchema for ColumnDefinition {
    fn validate(&self) -> Result<(), Error> {
        if let Self::WithAggregation(simple_column, aggregation_method) = self {
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
        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct SimpleColumn {
    datatype: Datatype,
    /// position in the sorting key (`ORDER BY`) in MergeTree tables
    /// which can be unterstood as a form of a primary key. Please consult
    /// https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/mergetree/
    /// for more
    order_key_position: Option<u8>,
}


impl SimpleColumn {
    pub fn new(datatype: Datatype, order_key_position: Option<u8>) -> Self {
        Self {
            datatype,
            order_key_position
        }
    }
}

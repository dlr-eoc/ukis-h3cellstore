use std::any::type_name;
use std::cmp::Ordering;
use std::collections::HashMap;

use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::colvec::Datatype;
use crate::common::{ordered_h3_resolutions, Named};
use crate::error::Error;
use crate::COL_NAME_H3INDEX;
use itertools::Itertools;

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
    name: String,
    table_engine: TableEngine,
    compression_method: CompressionMethod,
    h3_base_resolutions: Vec<u8>,
    h3_compacted_resolutions: Vec<u8>,
    temporal_resolution: TemporalResolution,
    temporal_partitioning: TemporalPartitioning,
    columns: HashMap<String, ColumnDefinition>,
    partition_by_columns: Vec<String>,
}

impl CompactedTableSchema {
    /// columns to use for the order-by of the table
    pub fn order_by_column_names(&self) -> Vec<String> {
        let default_key_pos = 10;
        self.columns
            .iter()
            .filter(|(column_name, def)| {
                def.order_key_position().is_some() || COL_NAME_H3INDEX == column_name.as_str()
            })
            .map(|(column_name, def)| {
                let key_pos = def.order_key_position().unwrap_or(default_key_pos) as i16;
                // always have the mandatory h3index first as the location is most certainly the
                // most important criteria for a fast lookup
                let pos = key_pos
                    - if column_name == COL_NAME_H3INDEX {
                        100
                    } else {
                        0
                    };

                (pos, column_name)
            })
            .sorted_by(|a, b| {
                match a.0.cmp(&b.0) {
                    Ordering::Less => Ordering::Less,
                    // sort by column name as second criteria, to have a repeatable ordering
                    Ordering::Equal => a.1.cmp(&b.1),
                    Ordering::Greater => Ordering::Greater,
                }
            })
            .map(|(_, column_name)| column_name.clone())
            .collect()
    }

    fn get_column_def(&self, column_name: &str) -> Result<ColumnDefinition, Error> {
        match self.columns.get(column_name) {
            Some(def) => Ok(def.clone()),
            None => {
                return Err(Error::SchemaValidationError(
                    type_name::<Self>(),
                    format!("mandatory column {} is missing", COL_NAME_H3INDEX),
                ))
            }
        }
    }

    pub fn h3index_column(&self) -> Result<(String, ColumnDefinition), Error> {
        let def = self.get_column_def(COL_NAME_H3INDEX)?;

        if ColumnDefinition::H3Index == def {
            Ok((COL_NAME_H3INDEX.to_string(), def))
        } else {
            return Err(Error::SchemaValidationError(
                type_name::<Self>(),
                format!(
                    "mandatory column {} is must be a h3index column",
                    COL_NAME_H3INDEX
                ),
            ));
        }
    }

    /// columns expressions to use the partitioning of the tables
    pub fn partition_by_expressions(&self) -> Result<Vec<String>, Error> {
        let (h3index_col_name, h3index_col_def) = self.h3index_column()?;

        let mut partition_by = vec![
            // h3index base cell is always the first
            partition_by_expression(
                &h3index_col_name,
                &h3index_col_def,
                &self.temporal_partitioning,
            ),
        ];

        if self.partition_by_columns.is_empty() {
            // attempt to use a time column for partitioning if there is one
            let mut new_partition_by_entries = vec![];
            for (column_name, def) in self.columns.iter() {
                if def.datatype().is_temporal() {
                    let partition_expr =
                        partition_by_expression(&column_name, &def, &self.temporal_partitioning);
                    if !new_partition_by_entries.contains(&partition_expr)
                        && !partition_by.contains(&partition_expr)
                    {
                        new_partition_by_entries.push(partition_expr);
                    }
                }
            }
            if new_partition_by_entries.len() > 1 {
                return Err(Error::SchemaValidationError(
                    type_name::<Self>(),
                    "found multiple temporal columns - explict specification of partitioning columns required".to_string()
                ));
            }
            partition_by.append(&mut new_partition_by_entries);
        } else {
            for column_name in self.partition_by_columns.iter() {
                let def = self.get_column_def(&column_name)?;
                let partition_expr =
                    partition_by_expression(&column_name, &def, &self.temporal_partitioning);
                if !partition_by.contains(&partition_expr) {
                    partition_by.push(partition_expr);
                }
            }
        }
        Ok(partition_by)
    }
}

/// generate a single partition expression for a single column
fn partition_by_expression(
    column_name: &str,
    def: &ColumnDefinition,
    temporal_partitioning: &TemporalPartitioning,
) -> String {
    match def {
        ColumnDefinition::H3Index => format!("h3GetBaseCell({})", column_name),
        ColumnDefinition::Simple(_) | ColumnDefinition::WithAggregation(_, _) => {
            if def.datatype().is_temporal() {
                match temporal_partitioning {
                    TemporalPartitioning::Year => format!("toString(toYear({}))", column_name),
                    TemporalPartitioning::Month => format!("toString(toMonth({}))", column_name),
                }
            } else {
                column_name.to_string()
            }
        }
    }
}

impl ValidateSchema for CompactedTableSchema {
    fn validate(&self) -> Result<(), Error> {
        validate_table_name(type_name::<Self>(), &self.name)?;
        self.compression_method.validate()?;

        // a h3index column must exist
        self.h3index_column()?;

        // validate table engine
        if let TableEngine::SummingMergeTree(sum_columns) = &self.table_engine {
            let missing_columns: Vec<_> = sum_columns
                .iter()
                .filter(|sum_column| !self.columns.contains_key(*sum_column))
                .cloned()
                .collect();
            if !missing_columns.is_empty() {
                return Err(Error::SchemaValidationError(
                    type_name::<TableEngine>(),
                    format!(
                        "SummingMergeTree engine is missing columns: {}",
                        missing_columns.join(", ")
                    ),
                ));
            }
        }

        // validate h3 resolutions
        let base_resolutions = ordered_h3_resolutions(&self.h3_base_resolutions)?;
        if base_resolutions.is_empty() {
            return Err(Error::SchemaValidationError(
                type_name::<Self>(),
                "at least one h3 base resolution is required".to_string(),
            ));
        }
        let compacted_resolutions = ordered_h3_resolutions(&self.h3_compacted_resolutions)?;
        if !compacted_resolutions.is_empty() {
            if let (Some(base_max), Some(compacted_max)) = (
                base_resolutions.iter().max(),
                compacted_resolutions.iter().max(),
            ) {
                if compacted_max > base_max {
                    return Err(Error::SchemaValidationError(
                        type_name::<Self>(),
                        "compacted h3 resolutions may not be greater than the max base resolution"
                            .to_string(),
                    ));
                }
            }
        }

        // a useful partitioning can be created
        self.partition_by_expressions()?;

        Ok(())
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

#[derive(Serialize, Deserialize, Debug, PartialEq)]
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

#[derive(Serialize, Deserialize, Debug, PartialEq)]
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
#[serde(rename_all = "lowercase")]
pub enum AggregationMethod {
    RelativeToResolutionArea,
    Sum,
    Max,
    Min,
    Average,
    // TODO: aggragation method to generate parent resolution for other h3index column
}

impl AggregationMethod {
    pub fn is_applicable_to_datatype(&self, datatype: &Datatype) -> bool {
        match self {
            Self::RelativeToResolutionArea => !(datatype.is_nullable() || datatype.is_temporal()),
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
            Self::RelativeToResolutionArea => "relativetoresolutionarea",
            Self::Max => "max",
            Self::Min => "min",
            Self::Sum => "sum",
            Self::Average => "average",
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
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

pub struct CompactedTableSchemaBuilder {
    schema: CompactedTableSchema,
}

impl CompactedTableSchemaBuilder {
    pub fn new(table_name: &str) -> Self {
        let mut columns = HashMap::new();
        columns.insert(COL_NAME_H3INDEX.to_string(), ColumnDefinition::H3Index);
        Self {
            schema: CompactedTableSchema {
                name: table_name.to_string(),
                table_engine: Default::default(),
                compression_method: Default::default(),
                h3_base_resolutions: vec![],
                h3_compacted_resolutions: vec![],
                temporal_resolution: Default::default(),
                temporal_partitioning: Default::default(),
                partition_by_columns: Default::default(),
                columns,
            },
        }
    }

    pub fn table_engine(mut self, table_engine: TableEngine) -> Self {
        self.schema.table_engine = table_engine;
        self
    }

    pub fn compression_method(mut self, compression_method: CompressionMethod) -> Self {
        self.schema.compression_method = compression_method;
        self
    }

    pub fn h3_base_resolutions(mut self, h3res: Vec<u8>) -> Self {
        self.schema.h3_base_resolutions = h3res;
        self
    }

    pub fn h3_compacted_resolutions(mut self, h3res: Vec<u8>) -> Self {
        self.schema.h3_compacted_resolutions = h3res;
        self
    }

    pub fn temporal_resolution(mut self, temporal_resolution: TemporalResolution) -> Self {
        self.schema.temporal_resolution = temporal_resolution;
        self
    }

    pub fn temporal_partitioning(mut self, temporal_partitioning: TemporalPartitioning) -> Self {
        self.schema.temporal_partitioning = temporal_partitioning;
        self
    }

    pub fn add_column(mut self, column_name: &str, def: ColumnDefinition) -> Self {
        self.schema.columns.insert(column_name.to_string(), def);
        self
    }

    pub fn partition_by(mut self, columns: Vec<String>) -> Self {
        self.schema.partition_by_columns = columns;
        self
    }

    pub fn build(self) -> Result<CompactedTableSchema, Error> {
        self.schema.validate()?;
        Ok(self.schema)
    }
}

#[cfg(test)]
mod tests {

    use crate::clickhouse::schema::{
        AggregationMethod, ColumnDefinition, CompactedTableSchema, CompactedTableSchemaBuilder,
        Schema, SimpleColumn, TemporalPartitioning,
    };
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

    fn data_okavango_delta() -> CompactedTableSchema {
        CompactedTableSchemaBuilder::new("okavango_delta")
            .h3_compacted_resolutions(vec![2, 3])
            .h3_base_resolutions(vec![1, 2, 3, 4, 5])
            .temporal_partitioning(TemporalPartitioning::Month)
            .add_column(
                "elephant_density",
                ColumnDefinition::WithAggregation(
                    SimpleColumn {
                        datatype: Datatype::F32,
                        order_key_position: None,
                    },
                    AggregationMethod::Average,
                ),
            )
            .add_column(
                "observed_on",
                ColumnDefinition::Simple(SimpleColumn {
                    datatype: Datatype::Date,
                    order_key_position: Some(0),
                }),
            )
            .build()
            .unwrap()
    }

    #[test]
    fn schema_to_json() {
        let s = Schema::CompactedTable(data_okavango_delta());
        println!("{}", s.to_json_string().unwrap());
    }

    #[test]
    fn partitioning_columns_implicit() {
        assert_eq!(
            data_okavango_delta().partition_by_expressions().unwrap(),
            vec![
                "h3GetBaseCell(h3index)".to_string(),
                "toString(toMonth(observed_on))".to_string()
            ]
        );
    }
}

use std::any::type_name;
use std::cmp::Ordering;

use itertools::Itertools;
use lazy_static::lazy_static;
pub use regex::Regex;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

pub use agg::AggregationMethod;
use arrow_h3::export::h3ron::collections::HashMap;
use arrow_h3::export::h3ron::H3_MAX_RESOLUTION;
pub use column::{ColumnDefinition, SimpleColumn};
pub use datatype::ClickhouseDataType;
pub use other::{CompressionMethod, TableEngine};
pub use temporal::{TemporalPartitioning, TemporalResolution};

use crate::clickhouse::compacted_tables::temporary_key::TemporaryKey;
use crate::clickhouse::compacted_tables::{Table, TableSpec, COL_NAME_H3INDEX};
use crate::Error;

pub mod agg;
pub mod column;
pub mod datatype;
pub mod other;
pub mod temporal;

pub trait ValidateSchema {
    fn validate(&self) -> Result<(), Error>;
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, PartialEq, Clone)]
pub struct CompactedTableSchema {
    pub name: String,
    table_engine: TableEngine,
    compression_method: CompressionMethod,
    pub(crate) h3_base_resolutions: Vec<u8>,
    pub max_h3_resolution: u8,
    pub(crate) use_compaction: bool,
    temporal_resolution: TemporalResolution,
    temporal_partitioning: TemporalPartitioning,
    pub(crate) columns: HashMap<String, ColumnDefinition>,
    partition_by_columns: Vec<String>,
    pub(crate) has_base_suffix: bool,
}

#[derive(Eq)]
pub(crate) struct ResolutionMetadata {
    h3_resolution: u8,
    is_compacted: bool,
}

impl ResolutionMetadata {
    #[inline]
    pub fn new(h3_resolution: u8, is_compacted: bool) -> Self {
        Self {
            h3_resolution,
            is_compacted,
        }
    }
}

impl PartialOrd for ResolutionMetadata {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ResolutionMetadata {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.h3_resolution < other.h3_resolution {
            Ordering::Less
        } else if self.h3_resolution > other.h3_resolution {
            Ordering::Greater
        } else if self.is_compacted == other.is_compacted {
            Ordering::Equal
        } else if self.is_compacted && !other.is_compacted {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    }
}

impl PartialEq for ResolutionMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.is_compacted == other.is_compacted && self.h3_resolution == other.h3_resolution
    }
}

impl ValidateSchema for CompactedTableSchema {
    fn validate(&self) -> Result<(), Error> {
        validate_table_name(type_name::<Self>(), &self.name)?;
        self.compression_method.validate()?;
        self.temporal_partitioning.validate()?;

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

impl CompactedTableSchema {
    pub(crate) fn build_table(
        &self,
        resolution_metadata: &ResolutionMetadata,
        temporary_key: &Option<TemporaryKey>,
    ) -> Table {
        Table {
            basename: self.name.clone(),
            spec: TableSpec {
                h3_resolution: resolution_metadata.h3_resolution,
                is_compacted: resolution_metadata.is_compacted,
                temporary_key: temporary_key.as_ref().map(|tk| tk.to_string()),
                has_base_suffix: self.has_base_suffix,
            },
        }
    }

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
                    Ordering::Equal => a.1.cmp(b.1),
                    Ordering::Greater => Ordering::Greater,
                }
            })
            .map(|(_, column_name)| column_name.clone())
            .collect()
    }

    fn get_column_definition(&self, column_name: &str) -> Result<ColumnDefinition, Error> {
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
        let def = self.get_column_definition(COL_NAME_H3INDEX)?;

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
                        partition_by_expression(column_name, def, &self.temporal_partitioning);
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
                let def = self.get_column_definition(column_name)?;
                let partition_expr =
                    partition_by_expression(column_name, &def, &self.temporal_partitioning);
                if !partition_by.contains(&partition_expr) {
                    partition_by.push(partition_expr);
                }
            }
        }
        Ok(partition_by)
    }

    pub(crate) fn get_resolution_metadata(&self) -> Result<Vec<ResolutionMetadata>, Error> {
        let compacted_resolutions: Vec<_> = if self.use_compaction {
            let max_res = *self
                .h3_base_resolutions
                .iter()
                .max()
                .ok_or(Error::MixedH3Resolutions)?; // TODO: better error
            (0..=max_res)
                .map(|r| ResolutionMetadata::new(r, true))
                .collect()
        } else {
            vec![]
        };
        Ok(self
            .h3_base_resolutions
            .iter()
            .cloned()
            .map(|r| ResolutionMetadata::new(r, false))
            .chain(compacted_resolutions)
            .collect())
    }

    fn build_create_statement(&self, table: &Table) -> Result<String, Error> {
        let partition_by = if table.spec.temporary_key.is_none() {
            // partitioning is only relevant for non-temporary tables
            Some(self.partition_by_expressions()?.join(", "))
        } else {
            None
        };
        let order_by = self.order_by_column_names().join(", ");
        let engine = match &self.table_engine {
            TableEngine::ReplacingMergeTree => "ReplacingMergeTree".to_string(),
            TableEngine::SummingMergeTree(smt_columns) => {
                format!("SummingMergeTree({})", smt_columns.join(", "))
            }
            TableEngine::AggregatingMergeTree => "AggregatingMergeTree".to_string(),
        };
        let codec = match &self.compression_method {
            CompressionMethod::LZ4HC(level) => format!("LZ4HC({})", level),
            CompressionMethod::ZSTD(level) => format!("ZSTD({})", level),
            CompressionMethod::Delta(delta_bytes) => format!("Delta({})", delta_bytes),
            CompressionMethod::DoubleDelta => "DoubleDelta".to_string(),
            CompressionMethod::Gorilla => "Gorilla".to_string(),
            CompressionMethod::T64 => "T64".to_string(),
        };
        let columns = &self
            .columns
            .iter()
            .sorted_by(|a, b| Ord::cmp(a.0, b.0)) // order to make the SQL comparable
            .map(|(col_name, def)| {
                format!(
                    " {} {} CODEC({})",
                    col_name,
                    def.datatype().sql_type_name(),
                    codec
                )
            })
            .join(",\n");

        Ok(format!(
            "CREATE TABLE IF NOT EXISTS {} ( {} ) ENGINE {} {} ORDER BY ({});",
            table.to_table_name(),
            columns,
            engine,
            partition_by.map_or_else(|| "".to_string(), |pb| format!("PARTITION BY ({})", pb)),
            order_by
        ))
    }

    pub fn build_create_statements(
        &self,
        temporary_key: &Option<TemporaryKey>,
    ) -> Result<Vec<String>, Error> {
        self.get_resolution_metadata()?
            .iter()
            .map(|resolution_metadata| {
                let table = self.build_table(resolution_metadata, temporary_key);
                self.build_create_statement(&table)
            })
            .collect::<Result<Vec<String>, Error>>()
    }

    pub fn build_drop_statements(
        &self,
        temporary_key: &Option<TemporaryKey>,
    ) -> Result<Vec<String>, Error> {
        Ok(self
            .get_resolution_metadata()?
            .iter()
            .map(|resolution_metadata| {
                let table = self.build_table(resolution_metadata, temporary_key);
                format!("drop table if exists {}", table.to_table_name())
            })
            .collect::<Vec<String>>())
    }
}

fn ordered_h3_resolutions(h3res_slice: &[u8]) -> Result<Vec<u8>, Error> {
    let mut cleaned = vec![];
    for res in h3res_slice.iter() {
        if res > &H3_MAX_RESOLUTION {
            return Err(Error::UnsupportedH3Resolution(*res));
        }
        cleaned.push(*res);
    }
    cleaned.sort_unstable();
    cleaned.dedup();
    Ok(cleaned)
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
                    TemporalPartitioning::Month => format!("toString(toMonth({}))", column_name),
                    TemporalPartitioning::Years(num_years) => {
                        if *num_years == 1 {
                            format!("toString(toYear({}))", column_name)
                        } else {
                            // reshaping the year according to num_years
                            //
                            // With num_years == 3, value '2019' will contain the years 2019, 2020 and 2021.
                            format!(
                                "toString(floor(toYear({})/{})*{})",
                                column_name, num_years, num_years
                            )
                        }
                    }
                }
            } else {
                column_name.to_string()
            }
        }
    }
}

#[derive(Clone)]
pub struct CompactedTableSchemaBuilder {
    schema: CompactedTableSchema,
    use_compaction: bool,
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
                max_h3_resolution: 0,
                use_compaction: true,
                temporal_resolution: Default::default(),
                temporal_partitioning: Default::default(),
                partition_by_columns: Default::default(),
                columns,
                has_base_suffix: true,
            },
            use_compaction: true,
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
        if !h3res.is_empty() {
            self.schema.max_h3_resolution = *(h3res
                .iter()
                .max()
                .expect("no resolutions to ge max res from"));
        }
        self.schema.h3_base_resolutions = h3res;
        self
    }

    pub fn use_compacted_resolutions(mut self, use_compaction: bool) -> Self {
        self.use_compaction = use_compaction;
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
    use crate::clickhouse::compacted_tables::schema::{
        validate_table_name, AggregationMethod, ClickhouseDataType, ColumnDefinition,
        CompactedTableSchema, CompactedTableSchemaBuilder, ResolutionMetadata, SimpleColumn,
        TemporalPartitioning,
    };

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
            .h3_base_resolutions(vec![1, 2, 3, 4, 5])
            .temporal_partitioning(TemporalPartitioning::Month)
            .add_column(
                "elephant_density",
                ColumnDefinition::WithAggregation(
                    SimpleColumn::new(ClickhouseDataType::Float32, None),
                    AggregationMethod::Average,
                ),
            )
            .add_column(
                "observed_on",
                ColumnDefinition::Simple(SimpleColumn::new(ClickhouseDataType::DateTime, Some(0))),
            )
            .build()
            .unwrap()
    }

    #[test]
    #[cfg(feature = "serde")]
    fn schema_json_roundtrip() {
        let s = data_okavango_delta();
        let json_string = serde_json::to_string(&s).unwrap();
        //println!("{}", json_string);
        let s2: CompactedTableSchema = serde_json::from_str(&json_string).unwrap();
        assert_eq!(s, s2);
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

    #[test]
    fn resolution_metadata_sort() {
        let mut v1 = vec![
            ResolutionMetadata::new(4, false),
            ResolutionMetadata::new(3, false),
        ];
        v1.sort_unstable();
        assert_eq!(v1[0].h3_resolution, 3);
        assert_eq!(v1[1].h3_resolution, 4);

        let mut v2 = vec![
            ResolutionMetadata::new(3, true),
            ResolutionMetadata::new(3, false),
        ];
        v2.sort_unstable();
        assert!(!v2[0].is_compacted);
        assert!(v2[1].is_compacted);
    }
}

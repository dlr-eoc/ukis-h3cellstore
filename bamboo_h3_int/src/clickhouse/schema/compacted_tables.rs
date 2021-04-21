use std::any::type_name;
use std::cmp::Ordering;
use std::collections::HashMap;

use clickhouse_rs::{Block, ClientHandle};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::clickhouse::compacted_tables::{Table, TableSpec};
use crate::clickhouse::schema::{
    validate_table_name, ColumnDefinition, CompressionMethod, CreateSchema, GetSchemaColumns,
    GetSqlType, TableEngine, TemporalPartitioning, TemporalResolution, ValidateSchema,
};
use crate::clickhouse::FromWithDatatypes;
use crate::common::ordered_h3_resolutions;
use crate::error::Error;
use crate::{ColumnSet, COL_NAME_H3INDEX};

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct CompactedTableSchema {
    pub(crate) name: String,
    table_engine: TableEngine,
    compression_method: CompressionMethod,
    pub(crate) h3_base_resolutions: Vec<u8>,
    pub(crate) use_compaction: bool,
    temporal_resolution: TemporalResolution,
    temporal_partitioning: TemporalPartitioning,
    columns: HashMap<String, ColumnDefinition>,
    partition_by_columns: Vec<String>,
    pub(crate) has_base_suffix: bool,
}

#[derive(Eq)]
struct ResolutionMetadata {
    h3_resolution: u8,
    is_compacted: bool,
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

    fn get_resolution_metadata(&self) -> Result<Vec<ResolutionMetadata>, Error> {
        let compacted_resolutions: Vec<_> = if self.use_compaction {
            let max_res = *self
                .h3_base_resolutions
                .iter()
                .max()
                .ok_or(Error::MixedResolutions)?; // TODO: better error
            (0..=max_res)
                .map(|r| ResolutionMetadata {
                    h3_resolution: r,
                    is_compacted: true,
                })
                .collect()
        } else {
            vec![]
        };
        Ok(self
            .h3_base_resolutions
            .iter()
            .cloned()
            .map(|r| ResolutionMetadata {
                h3_resolution: r,
                is_compacted: false,
            })
            .chain(compacted_resolutions)
            .collect())
    }
}

impl CreateSchema for CompactedTableSchema {
    fn create_statements(&self) -> Result<Vec<String>, Error> {
        let partition_by = self.partition_by_expressions()?.join(", ");
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
        };
        let columns = &self
            .columns
            .iter()
            .sorted_by(|a, b| Ord::cmp(a.0, b.0)) // order to make the SQL comparable
            .map(|(col_name, def)| {
                format!(
                    " {} {} CODEC({})",
                    col_name,
                    def.datatype().sqltype().to_string(),
                    codec
                )
            })
            .join(",\n");

        Ok(self
            .get_resolution_metadata()?
            .iter()
            .map(|resolution_metadata| {
                let table = Table {
                    basename: self.name.clone(),
                    spec: TableSpec {
                        h3_resolution: resolution_metadata.h3_resolution,
                        is_compacted: resolution_metadata.is_compacted,
                        temporary_key: None,
                        has_base_suffix: self.has_base_suffix,
                    },
                };

                format!(
                    "
CREATE TABLE IF NOT EXISTS {} (
{}
)
ENGINE {}
PARTITION BY ({})
ORDER BY ({});
",
                    table.to_table_name(),
                    columns,
                    engine,
                    partition_by,
                    order_by
                )
            })
            .collect())
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

        // a useful partitioning can be created
        self.partition_by_expressions()?;

        Ok(())
    }
}

impl GetSchemaColumns for CompactedTableSchema {
    fn get_columns(&self) -> HashMap<String, ColumnDefinition> {
        self.columns.clone()
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

pub struct CompactedTableInserter<'a> {
    client: &'a mut ClientHandle,
    schema: &'a CompactedTableSchema,
    schema_h3_max_res: Option<u8>,
}

impl<'a> CompactedTableInserter<'a> {
    pub fn new(client: &'a mut ClientHandle, schema: &'a CompactedTableSchema) -> Self {
        Self {
            client,
            schema,
            schema_h3_max_res: None,
        }
    }

    fn schema_h3_max_resolution(&mut self) -> Result<u8, Error> {
        match self.schema_h3_max_res {
            Some(r) => Ok(r),
            None => {
                let r = self
                    .schema
                    .h3_base_resolutions
                    .iter()
                    .max()
                    .cloned()
                    .ok_or(Error::MixedResolutions)?; // TODO: Better error
                self.schema_h3_max_res = Some(r);
                Ok(r)
            }
        }
    }

    fn build_table(
        &mut self,
        h3_resolution: u8,
        temporary_key: Option<String>,
    ) -> Result<Table, Error> {
        let table = Table {
            basename: self.schema.name.clone(),
            spec: TableSpec {
                h3_resolution,
                is_compacted: self.schema_h3_max_resolution()? != h3_resolution,
                temporary_key,
                has_base_suffix: self.schema.has_base_suffix,
            },
        };
        Ok(table)
    }

    async fn create_schema(&mut self) -> Result<(), Error> {
        for stmt in self.schema.create_statements()?.iter() {
            self.client.execute(stmt).await?;
        }
        Ok(())
    }

    pub async fn insert_columnset(&mut self, columnset: &ColumnSet) -> Result<(), Error> {
        let mut splitted = columnset
            .to_compacted(&COL_NAME_H3INDEX)?
            .split_by_resolution(&COL_NAME_H3INDEX, true)?;

        if splitted.is_empty() {
            return Ok(()); // nothing to save
        }

        let schema_max_res = self.schema_h3_max_resolution()?;

        // validate the received h3 resolutions
        for h3_res in splitted.keys() {
            if h3_res > &schema_max_res {
                log::error!(
                    "columnset included h3 resolution = {}, but the schema is only defined until {}",
                    h3_res,
                    schema_max_res
                );
                return Err(Error::InvalidH3Resolution(*h3_res));
            } else if h3_res < &schema_max_res && !self.schema.use_compaction {
                log::error!(
                    "columnset uses the max h3 resolution = {}, and does not allow compaction. Inserting h3 res = {} not possible",
                    schema_max_res,
                    h3_res
                );
                return Err(Error::InvalidH3Resolution(*h3_res));
            }
        }

        self.create_schema().await?;

        let target_datatypes: HashMap<_, _> = self
            .schema
            .get_columns()
            .drain()
            .map(|(col_name, col_def)| (col_name, col_def.datatype()))
            .collect();

        for (h3res, cs) in splitted.drain() {
            let table = self.build_table(h3res, None)?;
            self.client
                .insert(
                    table.to_table_name(),
                    Block::from_with_datatypes(cs, &target_datatypes)?,
                )
                .await?
        }

        // TODO: create other base_table data
        // TODO: deduplicate tables
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::clickhouse::schema::compacted_tables::{
        CompactedTableSchemaBuilder, ResolutionMetadata,
    };
    use crate::clickhouse::schema::{
        AggregationMethod, ColumnDefinition, CompactedTableSchema, CreateSchema, Schema,
        SimpleColumn, TemporalPartitioning,
    };
    use crate::columnset::Datatype;

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
                    datatype: Datatype::DateTime,
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
        for s in s.create_statements().unwrap().iter() {
            println!("{}", s);
        }
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
            ResolutionMetadata {
                h3_resolution: 4,
                is_compacted: false,
            },
            ResolutionMetadata {
                h3_resolution: 3,
                is_compacted: false,
            },
        ];
        v1.sort_unstable();
        assert_eq!(v1[0].h3_resolution, 3);
        assert_eq!(v1[1].h3_resolution, 4);

        let mut v2 = vec![
            ResolutionMetadata {
                h3_resolution: 3,
                is_compacted: true,
            },
            ResolutionMetadata {
                h3_resolution: 3,
                is_compacted: false,
            },
        ];
        v2.sort_unstable();
        assert_eq!(v2[0].is_compacted, false);
        assert_eq!(v2[1].is_compacted, true);
    }
}

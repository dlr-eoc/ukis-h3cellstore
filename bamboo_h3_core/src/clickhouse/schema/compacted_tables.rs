use std::any::type_name;
use std::cmp::Ordering;
use std::collections::HashMap;

use clickhouse_rs::{Block, ClientHandle};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tracing::{error, debug, warn};

use crate::clickhouse::compacted_tables::{Table, TableSpec};
use crate::clickhouse::schema::{
    validate_table_name, AggregationMethod, ColumnDefinition, CompressionMethod, CreateSchema,
    GetSchemaColumns, GetSqlType, TableEngine, TemporalPartitioning, TemporalResolution,
    ValidateSchema,
};
use crate::clickhouse::FromWithDatatypes;
use crate::common::ordered_h3_resolutions;
use crate::error::Error;
use crate::{ColumnSet, COL_NAME_H3INDEX};

/// the name of the parent h3index column used for aggregation
const COL_NAME_H3INDEX_PARENT_AGG: &str = "h3index_parent_agg";
const ALIAS_SOURCE_TABLE: &str = "src_table";

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct CompactedTableSchema {
    pub(crate) name: String,
    table_engine: TableEngine,
    compression_method: CompressionMethod,
    pub(crate) h3_base_resolutions: Vec<u8>,
    max_h3_resolution: u8,
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
    fn build_table(
        &self,
        resolution_metadata: &ResolutionMetadata,
        temporary_key: &Option<String>,
    ) -> Table {
        Table {
            basename: self.name.clone(),
            spec: TableSpec {
                h3_resolution: resolution_metadata.h3_resolution,
                is_compacted: resolution_metadata.is_compacted,
                temporary_key: temporary_key.clone(),
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

        Ok(format!(
            "
CREATE TABLE IF NOT EXISTS {} (
{}
)
ENGINE {}
{}
ORDER BY ({});
",
            table.to_table_name(),
            columns,
            engine,
            partition_by.map_or_else(|| "".to_string(), |pb| format!("PARTITION BY ({})", pb)),
            order_by
        ))
    }

    fn build_create_statements(
        &self,
        temporary_key: &Option<String>,
    ) -> Result<Vec<String>, Error> {
        self.get_resolution_metadata()?
            .iter()
            .map(|resolution_metadata| {
                let table = self.build_table(resolution_metadata, temporary_key);
                self.build_create_statement(&table)
            })
            .collect::<Result<Vec<String>, Error>>()
    }
}

impl CreateSchema for CompactedTableSchema {
    fn create_statements(&self) -> Result<Vec<String>, Error> {
        self.build_create_statements(&None)
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

pub struct CompactedTableInserter<'a> {
    client: &'a mut ClientHandle,
    schema: &'a CompactedTableSchema,
}

impl<'a> CompactedTableInserter<'a> {
    pub fn new(client: &'a mut ClientHandle, schema: &'a CompactedTableSchema) -> Self {
        Self { client, schema }
    }

    /// generate a temporary key to have out own tables to prepare the data in the db before
    /// moving it the the final tables
    fn generate_temporary_key(&self) -> String {
        uuid::Uuid::new_v4()
            .to_string()
            .replace("-", "")
            .to_lowercase()
    }

    async fn create_schema(&mut self) -> Result<(), Error> {
        for stmt in self.schema.create_statements()?.iter() {
            self.client.execute(stmt).await?;
        }
        Ok(())
    }

    pub async fn insert_columnset(&'a mut self, columnset: &ColumnSet) -> Result<(), Error> {
        let chunk_size = 500_000;
        let mut splitted = match columnset.to_compacted(&COL_NAME_H3INDEX) {
            Ok(cs) => cs.split_by_resolution_chunked(&COL_NAME_H3INDEX, true, Some(chunk_size)),
            Err(err) => match err {
                Error::MixedResolutions => {
                    // seems to be already compacted
                    columnset.split_by_resolution_chunked(&COL_NAME_H3INDEX, true, Some(chunk_size))
                }
                _ => return Err(err),
            },
        }?;

        if splitted.is_empty() {
            return Ok(()); // nothing to save
        }

        // validate the received h3 resolutions
        for h3_res in splitted.keys() {
            if h3_res > &self.schema.max_h3_resolution {
                error!(
                    "columnset included h3 resolution = {}, but the schema is only defined until {}",
                    h3_res,
                    self.schema.max_h3_resolution
                );
                return Err(Error::InvalidH3Resolution(*h3_res));
            } else if h3_res < &self.schema.max_h3_resolution && !self.schema.use_compaction {
                error!(
                    "columnset uses the max h3 resolution = {}, and does not allow compaction. Inserting h3 res = {} not possible",
                    self.schema.max_h3_resolution,
                    h3_res
                );
                return Err(Error::InvalidH3Resolution(*h3_res));
            }
        }

        self.create_schema().await?;

        let temporary_key = self.generate_temporary_key();

        let target_datatypes: HashMap<_, _> = self
            .schema
            .get_columns()
            .drain()
            .map(|(col_name, col_def)| (col_name, col_def.datatype()))
            .collect();

        let resolution_blocks = splitted
            .drain()
            .map(|(h3res, mut cs_vec)| {
                let blocks: Vec<_> = cs_vec
                    .drain(..)
                    .map(|cs| Block::from_with_datatypes(cs, &target_datatypes))
                    .collect::<Result<Vec<Block>, Error>>()?;
                Ok((h3res, blocks))
            })
            .collect::<Result<HashMap<u8, Vec<Block>>, Error>>()?;

        debug!(
            "Creating temporary tables for {} with temporary_key {}",
            self.schema.name,
            temporary_key
        );
        for stmt in self
            .schema
            .build_create_statements(&Some(temporary_key.clone()))?
            .iter()
        {
            self.client.execute(stmt).await?;
        }

        match self
            .insert_blocks(resolution_blocks, temporary_key.clone())
            .await
        {
            Ok(()) => {
                self.drop_temporary_tables(&Some(temporary_key.clone()))
                    .await?;
                Ok(())
            }
            Err(e) => {
                // attempt to restore the connection when it did break
                // todo: currently clickhouse_rs is unable to recover the connection and the
                //   following will only hide the true error, so it is commented out.
                // self.client.check_connection().await?;
                // self.drop_temporary_tables(temporary_key.clone()).await?;
                Err(e)
            }
        }
    }

    async fn insert_blocks(
        &mut self,
        mut resolution_blocks: HashMap<u8, Vec<Block>>,
        temporary_key: String,
    ) -> Result<(), Error> {
        let temporary_key_opt = Some(temporary_key.clone());
        for (h3res, mut blocks) in resolution_blocks.drain() {
            let resolution_metadata = ResolutionMetadata {
                h3_resolution: h3res,
                is_compacted: self.schema.max_h3_resolution != h3res,
            };
            let table_name = self
                .schema
                .build_table(&resolution_metadata, &temporary_key_opt)
                .to_table_name();
            for block in blocks.drain(..) {
                debug!(
                    "inserting a block of {} rows into {}",
                    block.row_count(),
                    table_name
                );
                self.client.insert(&table_name, block).await?
            }
        }
        let resolution_metadata_vec = self.schema.get_resolution_metadata()?;

        // apply all aggegations
        self.build_aggregated_resolutions(&temporary_key_opt)
            .await?;

        // copy data to the non-temporary tables to the final tables
        self.copy_data_from_temporary(&resolution_metadata_vec, &temporary_key_opt)
            .await?;

        // deduplicate tables
        self.deduplicate(&resolution_metadata_vec, &temporary_key_opt)
            .await?;
        Ok(())
    }

    async fn copy_data_from_temporary(
        &mut self,
        resolution_metadata_slice: &[ResolutionMetadata],
        temporary_key: &Option<String>,
    ) -> Result<(), Error> {
        if temporary_key.is_none() {
            // nothing to do without a temporary_key
            return Ok(());
        }
        let columns = self.schema.columns.keys().join(", ");
        for resolution_metadata in resolution_metadata_slice.iter() {
            let table_from = self
                .schema
                .build_table(resolution_metadata, temporary_key)
                .to_table_name();
            let table_to = self
                .schema
                .build_table(resolution_metadata, &None)
                .to_table_name();
            debug!("copying data from {} to {}", table_from, table_to);
            self.client
                .execute(format!(
                    "insert into {} ({}) select {} from {}",
                    table_to, columns, columns, table_from
                ))
                .await?;
        }
        Ok(())
    }

    async fn deduplicate(
        &mut self,
        resolution_metadata_slice: &[ResolutionMetadata],
        temporary_key: &Option<String>,
    ) -> Result<(), Error> {
        // this could also be implemented by obtaining the partition expression from
        // the clickhouse `system.parts` using a query like this one:
        //
        // `select name, partition_key from system.tables where name = 'timestamp_test_04_base';`
        //
        // that solution would be more resilient in case the schema description in this library
        // has diverged from the database tables.
        let part_expr = self.schema.partition_by_expressions()?;
        if part_expr.is_empty() || temporary_key.is_none() {
            // without a partitioning expression we got to deduplicate all partitions
            for resolution_metadata in resolution_metadata_slice.iter() {
                let table_final = self
                    .schema
                    .build_table(resolution_metadata, &None)
                    .to_table_name();
                debug!("de-duplicating the complete {} table :(", table_final);
                self.client
                    .execute(format!("optimize table {} deduplicate", table_final))
                    .await?;
            }
        } else {
            let part_expr_string = part_expr.iter().join(", ");
            for resolution_metadata in resolution_metadata_slice.iter() {
                let table_temp = self
                    .schema
                    .build_table(resolution_metadata, temporary_key)
                    .to_table_name();

                // obtain the list of relevant partitions which did receive changes by running
                // the partition expression on the temporary table.
                let block = self
                    .client
                    .query(format!(
                        "select distinct toString(({})) pe from {}",
                        part_expr_string, table_temp
                    ))
                    .fetch_all()
                    .await?;
                let mut partitions: Vec<String> = vec![];
                for row in block.rows() {
                    partitions.push(row.get("pe")?);
                }

                let table_final = self
                    .schema
                    .build_table(resolution_metadata, &None)
                    .to_table_name();
                for partition in partitions.iter() {
                    debug!(
                        "de-duplicating partition ({}) of the {} table",
                        partition,
                        table_final
                    );
                    self.client
                        .execute(format!(
                            "optimize table {} partition {} deduplicate",
                            table_final, partition
                        ))
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn drop_temporary_tables(&mut self, temporary_key: &Option<String>) -> Result<(), Error> {
        if temporary_key.is_none() {
            // nothing to do without a temporary_key
            return Ok(());
        }
        for resolution_metadata in self.schema.get_resolution_metadata()?.iter() {
            let table_name = self
                .schema
                .build_table(resolution_metadata, temporary_key)
                .to_table_name();
            debug!("dropping table {} when existing", &table_name);
            self.client
                .execute(format!("drop table if exists {}", table_name))
                .await?;
        }
        Ok(())
    }

    async fn build_aggregated_resolutions(
        &mut self,
        temporary_key: &Option<String>,
    ) -> Result<(), Error> {
        if temporary_key.is_none() {
            warn!("Aggregations can only build in temporary tables. Doing nothing ...");
            return Ok(());
        }
        let resolutions_to_aggregate: Vec<_> = self
            .schema
            .h3_base_resolutions
            .iter()
            .sorted()
            .rev()
            .cloned()
            .collect();
        if resolutions_to_aggregate.len() <= 1 {
            // having just one or zero resolutions require no aggregation
            return Ok(());
        }

        let column_names_with_aggregation: HashMap<_, _> = self
            .schema
            .get_columns()
            .drain()
            .map(|(col_name, def)| {
                let agg_method = match def {
                    ColumnDefinition::WithAggregation(_, agg_method) => Some(agg_method),
                    _ => None,
                };
                (col_name, agg_method)
            })
            .collect();

        let source_columns_expr = std::iter::once(COL_NAME_H3INDEX_PARENT_AGG)
            .chain(
                column_names_with_aggregation
                    .iter()
                    .filter(|(col_name, _)| col_name.as_str() != COL_NAME_H3INDEX)
                    .map(|(col_name, _)| col_name.as_str()),
            )
            .join(", ");

        let group_by_columns_expr = std::iter::once(format!(
            "{}.{}",
            ALIAS_SOURCE_TABLE, COL_NAME_H3INDEX_PARENT_AGG
        ))
        .chain(
            column_names_with_aggregation
                .iter()
                .filter(|(col_name, _)| col_name.as_str() != COL_NAME_H3INDEX)
                .filter_map(|(col_name, agg_opt)| {
                    // columns which are not used in aggragation are preserved as they are and are used
                    // for grouping.
                    if agg_opt.is_none() {
                        Some(format!("{}.{}", ALIAS_SOURCE_TABLE, col_name))
                    } else {
                        None
                    }
                }),
        )
        .join(", ");

        let insert_columns_expr = std::iter::once(COL_NAME_H3INDEX)
            .chain(
                column_names_with_aggregation
                    .iter()
                    .filter(|(col_name, _)| col_name.as_str() != COL_NAME_H3INDEX)
                    .map(|(col_name, _)| col_name.as_str()),
            )
            .join(", ");

        // TODO: switch to https://doc.rust-lang.org/std/primitive.slice.html#method.array_windows when that is stabilized.
        for agg_resolutions in resolutions_to_aggregate.windows(2) {
            let source_resolution = agg_resolutions[0];
            let target_resolution = agg_resolutions[1];
            debug!(
                "aggregating resolution {} into resolution {}",
                source_resolution,
                target_resolution
            );
            let target_table_name = self
                .schema
                .build_table(
                    &ResolutionMetadata {
                        h3_resolution: target_resolution,
                        is_compacted: false,
                    },
                    temporary_key,
                )
                .to_table_name();
            let source_tables: Vec<_> = {
                let mut source_tables = vec![
                    // the source base table
                    (
                        source_resolution,
                        self.schema
                            .build_table(
                                &ResolutionMetadata {
                                    h3_resolution: source_resolution,
                                    is_compacted: false,
                                },
                                temporary_key,
                            )
                            .to_table_name(),
                    ),
                ];

                // the compacted tables in between
                for r in (target_resolution + 1)..=source_resolution {
                    source_tables.push((
                        r,
                        self.schema
                            .build_table(
                                &ResolutionMetadata {
                                    h3_resolution: r,
                                    is_compacted: true,
                                },
                                temporary_key,
                            )
                            .to_table_name(),
                    ));
                }
                source_tables
            };

            let agg_columns_expr = std::iter::once(format!(
                "{}.{}",
                ALIAS_SOURCE_TABLE, COL_NAME_H3INDEX_PARENT_AGG
            ))
            .chain(
                column_names_with_aggregation
                    .iter()
                    .filter(|(col_name, _)| col_name.as_str() != COL_NAME_H3INDEX)
                    .map(|(col_name, agg_opt)| {
                        if let Some(agg) = agg_opt {
                            match agg {
                                AggregationMethod::RelativeToCellArea => {
                                    format!(
                                        "(sum({}.{}) / length(h3ToChildren({}.{}, {}))) as {}",
                                        ALIAS_SOURCE_TABLE,
                                        col_name,
                                        ALIAS_SOURCE_TABLE,
                                        COL_NAME_H3INDEX_PARENT_AGG,
                                        source_resolution,
                                        col_name,
                                    )
                                }
                                AggregationMethod::Sum => {
                                    format!(
                                        "sum({}.{}) as {}",
                                        ALIAS_SOURCE_TABLE, col_name, col_name
                                    )
                                }
                                AggregationMethod::Max => {
                                    // the max value. does not include child-cells not contained in the table
                                    format!(
                                        "max({}.{}) as {}",
                                        ALIAS_SOURCE_TABLE, col_name, col_name
                                    )
                                }
                                AggregationMethod::Min => {
                                    // the min value. does not include child-cells not contained in the table
                                    format!(
                                        "min({}.{}) as {}",
                                        ALIAS_SOURCE_TABLE, col_name, col_name
                                    )
                                }
                                AggregationMethod::Average => {
                                    // the avg value. does not include child-cells not contained in the table
                                    format!(
                                        "avg({}.{}) as {}",
                                        ALIAS_SOURCE_TABLE, col_name, col_name
                                    )
                                }
                            }
                        } else {
                            format!("{}.{}", ALIAS_SOURCE_TABLE, col_name)
                        }
                    }),
            )
            .join(", ");

            // estimate a number of batches to use to avoid moving large amounts of data at once. This slows
            // things down a bit, but helps when not too much memory is available on the db server.
            let num_batches = {
                let subqueries: Vec<_> = source_tables
                    .iter()
                    .map(|(_, table_name)| format!("(select count(*) from {})", table_name))
                    .collect();
                let q = format!("select ({})", subqueries.join(" + "));
                if let Some(row) = self.client.query(q).fetch_all().await?.rows().next() {
                    let num_rows: u64 = row.get(0)?;
                    (num_rows as usize / 1_000_000) + 1
                } else {
                    1_usize
                }
            };
            debug!("using {} batches for aggregation", num_batches);

            // append a parent index column to use for the aggregation to the source tables
            for (_, table_name) in source_tables.iter() {
                self.client
                    .execute(format!(
                    "alter table {} add column if not exists {} UInt64 default h3ToParent({},{})", 
                    table_name,
                    COL_NAME_H3INDEX_PARENT_AGG,
                    COL_NAME_H3INDEX,
                    target_resolution
                ))
                    .await?;
            }

            for batch in 0..num_batches {
                // batching is to be used on the parent indexes to always aggregate everything belonging
                // in the same row in the same batch. this ensures nothing get overwritten.
                let batching_expr = if num_batches > 1 {
                    format!(
                        "where modulo({}, {}) = {}",
                        COL_NAME_H3INDEX_PARENT_AGG, num_batches, batch
                    )
                } else {
                    "".to_string()
                };

                let source_select_expr = source_tables
                    .iter()
                    .map(|(_, source_table_name)| {
                        format!(
                            "select {} from {} FINAL {}",
                            source_columns_expr, source_table_name, batching_expr
                        )
                    })
                    .join("\n union all \n");

                let agg_expr = format!(
                    "
insert into {} ({})
select {}
from (
{}
) {}
group by {}",
                    target_table_name,
                    insert_columns_expr,
                    agg_columns_expr,
                    source_select_expr,
                    ALIAS_SOURCE_TABLE,
                    group_by_columns_expr
                );
                //dbg!(&agg_expr);
                self.client.execute(agg_expr).await?;
            }
        }
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

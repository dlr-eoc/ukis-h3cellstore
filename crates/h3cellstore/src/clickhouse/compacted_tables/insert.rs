use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use itertools::Itertools;
use tokio::task::spawn_blocking;
use tracing::{debug, debug_span, error, trace_span, Instrument};

use arrow_h3::algo::{Compact, SplitByH3Resolution};
use arrow_h3::H3DataFrame;
use clickhouse_arrow_grpc::{ArrowInterface, QueryInfo};

use crate::clickhouse::compacted_tables::optimize::{
    deduplicate_full, deduplicate_partitions_based_on_temporary_tables,
};
use crate::clickhouse::compacted_tables::schema::{
    AggregationMethod, ColumnDefinition, CompactedTableSchema, ResolutionMetadata,
};
use crate::clickhouse::compacted_tables::temporary_key::TemporaryKey;
use crate::clickhouse::compacted_tables::{CompactedTablesStore, COL_NAME_H3INDEX};
use crate::clickhouse::H3CellStore;
use crate::Error;

/// the name of the parent h3index column used for aggregation
const COL_NAME_H3INDEX_PARENT_AGG: &str = "h3index_parent_agg";
const ALIAS_SOURCE_TABLE: &str = "src_table";

#[derive(Debug, Clone)]
pub struct InsertOptions {
    pub create_schema: bool,
    pub deduplicate_after_insert: bool,
    pub max_num_rows_per_chunk: usize,

    /// boalean to set to true to abort the insert process
    pub abort: Arc<Mutex<bool>>,
}

impl Default for InsertOptions {
    fn default() -> Self {
        Self {
            create_schema: true,
            deduplicate_after_insert: true,
            max_num_rows_per_chunk: 1_000_000,
            abort: Arc::new(Mutex::new(false)),
        }
    }
}

pub struct Inserter<C> {
    store: C,
    schema: CompactedTableSchema,
    temporary_key: TemporaryKey,
    database_name: String,
    options: InsertOptions,
}

impl<C> Inserter<C>
where
    C: ArrowInterface + CompactedTablesStore + Send + Sync,
{
    pub fn new(
        store: C,
        schema: CompactedTableSchema,
        database_name: String,
        options: InsertOptions,
    ) -> Self {
        Self {
            store,
            schema,
            temporary_key: Default::default(),
            database_name,
            options,
        }
    }

    fn check_for_abort(&self) -> Result<(), Error> {
        let guard = self
            .options
            .abort
            .lock()
            .map_err(|_| Error::AcquiringLockFailed)?;
        if *guard {
            Err(Error::Abort)
        } else {
            Ok(())
        }
    }

    /// This method is a somewhat expensive operation
    pub async fn insert(&mut self, h3df: H3DataFrame) -> Result<(), Error> {
        let frames_by_resolution = if h3df.dataframe.is_empty() {
            Default::default()
        } else {
            let frames_by_resolution = spawn_blocking(move || {
                h3df.compact()
                    .and_then(|compacted| compacted.split_by_h3_resolution())
            })
            .await??;

            // somewhat validate the resolution range
            let max_res_found = frames_by_resolution
                .iter()
                .map(|(res, _)| *res)
                .max()
                .ok_or(Error::EmptyCells)?;
            let max_res_supported = self.schema.max_h3_resolution;
            if max_res_supported < max_res_found {
                error!("dataframe contains higher resolution ({}) than are supported in the tableset ({})", max_res_found, max_res_supported);
                return Err(Error::UnsupportedH3Resolution(max_res_found));
            }
            frames_by_resolution
        };
        self.check_for_abort()?;

        if frames_by_resolution.is_empty()
            || frames_by_resolution
                .iter()
                .all(|(_, h3df)| h3df.dataframe.is_empty())
        {
            // no data to insert, so exit early
            return Ok(());
        }
        let tk_str = self.temporary_key.to_string();
        let tk_opt = Some(self.temporary_key.clone());

        if self.options.create_schema {
            self.store
                .create_tableset(&self.database_name, &self.schema)
                .await?;
        }

        // ensure the temporary schema exists
        self.check_for_abort()?;
        self.create_temporary_tables()
            .instrument(debug_span!(
                "Creating temporary tables used for insert",
                temporary_key = tk_str.as_str()
            ))
            .await?;

        // insert into temporary tables
        for (h3_resolution, h3df) in frames_by_resolution {
            let table = self.schema.build_table(
                &ResolutionMetadata::new(
                    h3_resolution,
                    h3_resolution != self.schema.max_h3_resolution,
                ),
                &tk_opt,
            );

            self.check_for_abort()?;
            self.store
                .insert_h3dataframe_chunked(
                    self.database_name.as_str(),
                    table.to_table_name(),
                    h3df,
                    self.options.max_num_rows_per_chunk,
                )
                .await?;
        }

        let resolution_metadata = self.schema.get_resolution_metadata()?;

        // generate other resolutions and apply aggregations
        self.check_for_abort()?;
        self.write_aggregated_resolutions()
            .instrument(debug_span!(
                "Writing aggregated resolutions",
                temporary_key = tk_str.as_str()
            ))
            .await?;

        // move rows to non-temporary tables
        self.check_for_abort()?;
        self.copy_data_from_temporary(&resolution_metadata)
            .instrument(debug_span!(
                "Copying data from temporary tables",
                temporary_key = tk_str.as_str()
            ))
            .await?;

        // deduplicate
        self.check_for_abort()?;
        if self.options.deduplicate_after_insert {
            if let Err(e) = deduplicate_partitions_based_on_temporary_tables(
                &mut self.store,
                &self.database_name,
                &self.schema,
                &resolution_metadata,
                &tk_opt,
            )
            .instrument(debug_span!(
                "De-duplicating touched partitions",
                temporary_key = tk_str.as_str()
            ))
            .await
            {
                match e {
                    Error::MissingPrecondidtionsForPartialOptimization => {
                        deduplicate_full(
                            &mut self.store,
                            &self.database_name,
                            &self.schema,
                            &resolution_metadata,
                        )
                        .instrument(debug_span!(
                            "De-duplicating complete tables",
                            temporary_key = tk_str.as_str()
                        ))
                        .await?
                    }
                    _ => return Err(e),
                }
            }
        }

        Ok(())
    }

    async fn create_temporary_tables(&mut self) -> Result<(), Error> {
        self.drop_temporary_tables().await?; // drop them to be sure they are empty.
        for create_stmt in self
            .schema
            .build_create_statements(&Some(self.temporary_key.clone()))?
        {
            self.store
                .execute_query_checked(QueryInfo {
                    query: create_stmt,
                    database: self.database_name.clone(),
                    ..Default::default()
                })
                .await?;
        }
        Ok(())
    }

    async fn drop_temporary_tables(&mut self) -> Result<(), Error> {
        let mut finish_result = Ok(());
        // remove the temporary tables
        for drop_stmt in self
            .schema
            .build_drop_statements(&Some(self.temporary_key.clone()))?
        {
            if let Err(e) = self
                .store
                .execute_query_checked(QueryInfo {
                    query: drop_stmt,
                    database: self.database_name.clone(),
                    ..Default::default()
                })
                .await
            {
                error!(
                    "Dropping temporary table failed - skipping this one: {:?}",
                    e
                );

                // attempt to drop as many tables as possible in case errors occur.
                // Return the first encountered error
                if finish_result.is_ok() {
                    finish_result = Err(e.into())
                }
            }
        }
        finish_result
    }

    async fn write_aggregated_resolutions(&mut self) -> Result<(), Error> {
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
            .columns
            .iter()
            .map(|(col_name, def)| {
                let agg_method = match def {
                    ColumnDefinition::WithAggregation(_, agg_method) => Some(agg_method),
                    _ => None,
                };
                (col_name, agg_method)
            })
            .collect();

        let group_by_columns_expr = std::iter::once(format!(
            "{}.{}",
            ALIAS_SOURCE_TABLE, COL_NAME_H3INDEX_PARENT_AGG
        ))
        .chain(
            column_names_with_aggregation
                .iter()
                .filter(|(col_name, _)| col_name.as_str() != COL_NAME_H3INDEX)
                .filter_map(|(col_name, agg_opt)| {
                    // columns which are not used in aggregation are preserved as they are and are used
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

        let temporary_key = Some(self.temporary_key.clone());

        // TODO: switch to https://doc.rust-lang.org/std/primitive.slice.html#method.array_windows when that is stabilized.
        for agg_resolutions in resolutions_to_aggregate.windows(2) {
            self.check_for_abort()?;
            let source_resolution = agg_resolutions[0];
            let target_resolution = agg_resolutions[1];
            debug!(
                "aggregating resolution {} into resolution {}",
                source_resolution, target_resolution
            );
            let target_table_name = self
                .schema
                .build_table(
                    &ResolutionMetadata::new(target_resolution, false),
                    &temporary_key,
                )
                .to_table_name();
            let source_tables: Vec<_> = {
                let mut source_tables = vec![
                    // the source base table
                    (
                        source_resolution,
                        self.schema
                            .build_table(
                                &ResolutionMetadata::new(source_resolution, false),
                                &temporary_key,
                            )
                            .to_table_name(),
                    ),
                ];

                // the compacted tables in between.
                for r in target_resolution..=source_resolution {
                    if resolutions_to_aggregate.contains(&r) {
                        source_tables.push((
                            r,
                            self.schema
                                .build_table(&ResolutionMetadata::new(r, true), &temporary_key)
                                .to_table_name(),
                        ));
                    }
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
                let query_df = self
                    .store
                    .execute_into_dataframe(QueryInfo {
                        query: format!("select ({}) as num_rows", subqueries.join(" + ")),
                        database: self.database_name.clone(),
                        ..Default::default()
                    })
                    .await?;
                let num_rows_opt = query_df
                    .column("num_rows")?
                    .u64()?
                    .into_iter()
                    .next()
                    .map(|num_rows| num_rows.map(|v| (v as usize / 1_000_000) + 1));

                num_rows_opt.flatten().unwrap_or(1usize)
            };
            debug!("using {} batches for aggregation", num_batches);

            let source_columns_expr = std::iter::once(COL_NAME_H3INDEX_PARENT_AGG.to_string())
                .chain(
                    column_names_with_aggregation
                        .iter()
                        .filter(|(col_name, _)| col_name.as_str() != COL_NAME_H3INDEX)
                        .map(|(col_name, agg_opt)| {
                            match agg_opt {
                                Some(AggregationMethod::RelativeToCellArea) => {
                                    // correct the value so the division through number of children of the outer query
                                    // returns the correct result
                                    format!("if(h3GetResolution({}) = {}, {} * length(h3ToChildren({}, {})), {}) as {}",
                                            COL_NAME_H3INDEX, target_resolution ,col_name, COL_NAME_H3INDEX, source_resolution, col_name, col_name)
                                }
                                _ => col_name.to_string(),
                            }
                        }),
                )
                .join(", ");

            // append a parent index column to use for the aggregation to the source tables
            for (_, table_name) in source_tables.iter() {
                self.store
                    .execute_query_checked(QueryInfo {
                        query: format!(
                    "alter table {} add column if not exists {} UInt64 default h3ToParent({},{})",
                    table_name,
                    COL_NAME_H3INDEX_PARENT_AGG,
                    COL_NAME_H3INDEX,
                    target_resolution
                    ),
                        database: self.database_name.clone(),
                        ..Default::default()
                    })
                    .await?;
            }

            for batch in 0..num_batches {
                self.check_for_abort()?;
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

                self.store
                    .execute_query_checked(QueryInfo {
                        query: format!(
                            "insert into {} ({}) select {} from ( {} ) {} group by {}",
                            target_table_name,
                            insert_columns_expr,
                            agg_columns_expr,
                            source_select_expr,
                            ALIAS_SOURCE_TABLE,
                            group_by_columns_expr
                        ),
                        database: self.database_name.clone(),
                        ..Default::default()
                    })
                    .await?;
            }
        }
        Ok(())
    }

    async fn copy_data_from_temporary(
        &mut self,
        resolution_metadata_slice: &[ResolutionMetadata],
    ) -> Result<(), Error> {
        let columns = self.schema.columns.keys().join(", ");
        let tk = Some(self.temporary_key.clone());
        for resolution_metadata in resolution_metadata_slice.iter() {
            self.check_for_abort()?;
            let table_from = self
                .schema
                .build_table(resolution_metadata, &tk)
                .to_table_name();
            let table_to = self
                .schema
                .build_table(resolution_metadata, &None)
                .to_table_name();
            self.store
                .execute_query_checked(QueryInfo {
                    query: format!(
                        "insert into {} ({}) select {} from {}",
                        table_to, columns, columns, table_from
                    ),
                    database: self.database_name.clone(),
                    ..Default::default()
                })
                .instrument(trace_span!(
                    "copying data from temporary table to final table",
                    table_from = table_from.as_str(),
                    table_to = table_to.as_str()
                ))
                .await?;
        }
        Ok(())
    }

    pub async fn finish(mut self) -> Result<(), Error> {
        let tk_str = self.temporary_key.to_string();
        self.drop_temporary_tables()
            .instrument(debug_span!(
                "Dropping temporary tables used for insert",
                temporary_key = tk_str.as_str()
            ))
            .await
    }
}

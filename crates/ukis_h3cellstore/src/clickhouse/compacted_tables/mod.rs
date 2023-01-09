use std::cmp::Ordering;
use std::default::Default;

use async_trait::async_trait;
use tokio::task::spawn_blocking;
use tracing::{debug, info_span, warn, Instrument};

use h3ron::collections::{H3CellSet, HashMap};
use h3ron::iter::change_resolution;
use h3ron::{H3Cell, Index};
use h3ron_polars::frame::H3DataFrame;
use itertools::join;
use polars::prelude::{DataFrame, NamedFrom, Series};
pub use tableset::{Table, TableSet, TableSpec};
use ukis_clickhouse_arrow_grpc::{ArrowInterface, QueryInfo};

pub use crate::clickhouse::compacted_tables::insert::InsertOptions;
use crate::clickhouse::compacted_tables::insert::Inserter;
use crate::clickhouse::compacted_tables::optimize::deduplicate_full;
use crate::clickhouse::compacted_tables::schema::CompactedTableSchema;
use crate::clickhouse::compacted_tables::select::BuildCellQueryString;
pub use crate::clickhouse::compacted_tables::select::TableSetQuery;
use crate::clickhouse::compacted_tables::tableset::{find_tablesets, LoadTableSet};
use crate::Error;

mod insert;
mod optimize;
pub mod schema;
mod select;
pub mod tableset;
pub mod temporary_key;
pub mod traversal;

/// the column name which must be used for h3indexes.
pub const COL_NAME_H3INDEX: &str = "h3index";

pub struct QueryOptions {
    pub query: TableSetQuery,
    pub cells: Vec<H3Cell>,
    pub h3_resolution: u8,
    pub do_uncompact: bool,
}

impl QueryOptions {
    pub fn new(query: TableSetQuery, cells: Vec<H3Cell>, h3_resolution: u8) -> Self {
        // TODO: make cells an iterator with borrow and normalize to `h3_resolution`
        Self {
            query,
            cells,
            h3_resolution,
            do_uncompact: true,
        }
    }
}

#[async_trait]
pub trait CompactedTablesStore {
    async fn list_tablesets<S>(
        &mut self,
        database_name: S,
    ) -> Result<HashMap<String, TableSet>, Error>
    where
        S: AsRef<str> + Sync + Send;

    async fn get_tableset<S1, S2>(
        &mut self,
        database_name: S1,
        tableset_name: S2,
    ) -> Result<TableSet, Error>
    where
        S1: AsRef<str> + Sync + Send,
        S2: AsRef<str> + Send + Sync,
    {
        self.list_tablesets(database_name)
            .await?
            .remove(tableset_name.as_ref())
            .ok_or_else(|| Error::TableSetNotFound(tableset_name.as_ref().to_string()))
    }

    async fn drop_tableset<S, TS>(&mut self, database_name: S, tableset: TS) -> Result<(), Error>
    where
        S: AsRef<str> + Send + Sync,
        TS: LoadTableSet + Send + Sync;

    async fn create_tableset<S>(
        &mut self,
        database_name: S,
        schema: &CompactedTableSchema,
    ) -> Result<(), Error>
    where
        S: AsRef<str> + Sync + Send;

    async fn insert_h3dataframe_into_tableset<S>(
        &mut self,
        database_name: S,
        schema: &CompactedTableSchema,
        h3df: H3DataFrame<H3Cell>,
        options: InsertOptions,
    ) -> Result<(), Error>
    where
        S: AsRef<str> + Sync + Send;

    async fn deduplicate_schema<S>(
        &mut self,
        database_name: S,
        schema: &CompactedTableSchema,
    ) -> Result<(), Error>
    where
        S: AsRef<str> + Sync + Send;

    async fn query_tableset_cells<S, TS>(
        &mut self,
        database_name: S,
        tableset: TS,
        query_options: QueryOptions,
    ) -> Result<H3DataFrame<H3Cell>, Error>
    where
        S: AsRef<str> + Send + Sync,
        TS: LoadTableSet + Send + Sync;

    /// get stats about the number of cells and compacted cells in all the
    /// resolutions of the tableset
    async fn tableset_stats<S, TS>(
        &mut self,
        database_name: S,
        tableset: TS,
    ) -> Result<DataFrame, Error>
    where
        S: AsRef<str> + Send + Sync,
        TS: LoadTableSet + Send + Sync;
}

#[async_trait]
impl<C> CompactedTablesStore for C
where
    C: ArrowInterface + Send + Clone + Sync,
{
    async fn list_tablesets<S>(
        &mut self,
        database_name: S,
    ) -> Result<HashMap<String, TableSet>, Error>
    where
        S: AsRef<str> + Sync + Send,
    {
        let mut tablesets = {
            let tableset_df = self
                .execute_into_dataframe(QueryInfo {
                    query: format!(
                        "select table from system.columns where name = '{}' and database = currentDatabase()",
                        COL_NAME_H3INDEX
                    ),
                    database: database_name.as_ref().to_string(),
                    ..Default::default()
                })
                .await?;

            let tablenames: Vec<String> = tableset_df
                .column("table")?
                .utf8()?
                .into_iter()
                .flatten()
                .map(|table_name| table_name.to_string())
                .collect();
            find_tablesets(&tablenames)
        };

        // find the columns for the tablesets
        for (ts_name, ts) in tablesets.iter_mut() {
            let set_table_names = itertools::join(
                ts.tables()
                    .iter()
                    .map(|t| format!("'{}'", t.to_table_name())),
                ", ",
            );

            let columns_df = self
                .execute_into_dataframe(QueryInfo {
                    query: format!(
                        "select name, type, count(*) as c
                from system.columns
                where table in ({})
                and database = currentDatabase()
                and not startsWith(name, '{}')
                group by name, type",
                        set_table_names, COL_NAME_H3INDEX
                    ),
                    database: database_name.as_ref().to_string(),
                    ..Default::default()
                })
                .await?;

            for ((column_name, table_count_with_column), column_type) in columns_df
                .column("name")?
                .utf8()?
                .into_iter()
                .zip(columns_df.column("c")?.u64()?.into_iter())
                .zip(columns_df.column("type")?.utf8()?.into_iter())
            {
                if let (Some(column_name), Some(table_count_with_column), Some(column_type)) =
                    (column_name, table_count_with_column, column_type)
                {
                    // column must be present in all tables of the set, or it is not usable
                    if table_count_with_column == ts.num_tables() as u64 {
                        ts.columns
                            .insert(column_name.to_string(), column_type.to_string());
                    } else {
                        warn!("column {} is not present using the same type in all tables of set {}. ignoring this column", column_name, ts_name);
                    }
                }
            }
        }
        Ok(tablesets)
    }

    async fn drop_tableset<S, TS>(&mut self, database_name: S, tableset: TS) -> Result<(), Error>
    where
        S: AsRef<str> + Send + Sync,
        TS: LoadTableSet + Send + Sync,
    {
        return match tableset
            .load_tableset_from_store(self, database_name.as_ref())
            .await
        {
            Ok(tableset) => {
                for table in tableset
                    .base_tables()
                    .iter()
                    .chain(tableset.compacted_tables().iter())
                {
                    self.execute_query_checked(QueryInfo {
                        query: format!("drop table if exists {}", table.to_table_name()),
                        database: database_name.as_ref().to_string(),
                        ..Default::default()
                    })
                    .await?;
                }
                Ok(())
            }
            Err(e) => match e {
                Error::TableSetNotFound(_) => Ok(()),
                _ => Err(e),
            },
        };
    }

    async fn create_tableset<S>(
        &mut self,
        database_name: S,
        schema: &CompactedTableSchema,
    ) -> Result<(), Error>
    where
        S: AsRef<str> + Sync + Send,
    {
        for stmt in schema.build_create_statements(&None)? {
            self.execute_query_checked(QueryInfo {
                query: stmt,
                database: database_name.as_ref().to_string(),
                ..Default::default()
            })
            .await?;
        }
        Ok(())
    }

    async fn insert_h3dataframe_into_tableset<S>(
        &mut self,
        database_name: S,
        schema: &CompactedTableSchema,
        h3df: H3DataFrame<H3Cell>,
        options: InsertOptions,
    ) -> Result<(), Error>
    where
        S: AsRef<str> + Sync + Send,
    {
        if h3df.dataframe().is_empty() {
            return Ok(());
        }

        let h3df_shape = h3df.dataframe().shape();

        let mut inserter = Inserter::new(
            self.clone(),
            schema.clone(),
            database_name.as_ref().to_string(),
            options,
        );
        let insert_result = inserter
            .insert(h3df)
            .instrument(info_span!(
                "Inserting CellFrame into tableset",
                num_rows = h3df_shape.0,
                num_cols = h3df_shape.1,
                schema = schema.name.as_str(),
            ))
            .await;

        // always attempt to cleanup regardless if how the insert went
        let finish_result = inserter
            .finish()
            .instrument(info_span!(
                "Finishing CellFrame inserter",
                num_rows = h3df_shape.0,
                num_cols = h3df_shape.1,
                schema = schema.name.as_str(),
            ))
            .await;

        // return the earliest-occurred error
        if insert_result.is_err() {
            insert_result
        } else {
            finish_result
        }
    }

    async fn deduplicate_schema<S>(
        &mut self,
        database_name: S,
        schema: &CompactedTableSchema,
    ) -> Result<(), Error>
    where
        S: AsRef<str> + Sync + Send,
    {
        let resolution_metadata = schema.get_resolution_metadata()?;
        deduplicate_full(self, database_name, schema, &resolution_metadata)
            .instrument(info_span!(
                "De-duplicating complete schema",
                schema = schema.name.as_str()
            ))
            .await
    }

    async fn query_tableset_cells<S, TS>(
        &mut self,
        database_name: S,
        tableset: TS,
        query_options: QueryOptions,
    ) -> Result<H3DataFrame<H3Cell>, Error>
    where
        S: AsRef<str> + Send + Sync,
        TS: LoadTableSet + Send + Sync,
    {
        let tableset = tableset
            .load_tableset_from_store(self, database_name.as_ref())
            .await?;

        let (query_string, cells) = spawn_blocking(move || {
            query_options
                .query
                .build_cell_query_string(
                    &tableset,
                    query_options.h3_resolution,
                    &query_options.cells,
                )
                .map(|query_string| (query_string, query_options.cells))
        })
        .await??;

        let df = self
            .execute_into_dataframe(QueryInfo {
                query: query_string,
                database: database_name.as_ref().to_string(),
                ..Default::default()
            })
            .await?;
        let h3df = H3DataFrame::from_dataframe(df, COL_NAME_H3INDEX)?;

        let out_h3df = if query_options.do_uncompact {
            debug!(
                "Un-compacting queried H3DataFrame to target_resolution {}",
                query_options.h3_resolution
            );
            spawn_blocking(move || uncompact(h3df, cells, query_options.h3_resolution)).await??
        } else {
            debug!("returning queried H3DataFrame without un-compaction");
            h3df
        };
        Ok(out_h3df)
    }

    async fn tableset_stats<S, TS>(
        &mut self,
        database_name: S,
        tableset: TS,
    ) -> Result<DataFrame, Error>
    where
        S: AsRef<str> + Send + Sync,
        TS: LoadTableSet + Send + Sync,
    {
        let tableset = tableset
            .load_tableset_from_store(self, database_name.as_ref())
            .await?;

        let compacted_counts = {
            let df = self
                .execute_into_dataframe(compacted_counts_stmt(&tableset, database_name.as_ref()))
                .await?;

            let cc: Vec<_> = df
                .column("r")?
                .u8()?
                .into_iter()
                .zip(df.column("num_cells_stored_compacted")?.u64()?.into_iter())
                .map(|(r, count)| (r.unwrap(), count.unwrap()))
                .collect();
            cc
        };

        let mut df = self
            .execute_into_dataframe(uncompacted_counts_stmt(&tableset, database_name.as_ref()))
            .await?;

        let mut num_cells_stored_compacted = Vec::with_capacity(df.shape().0);
        let mut num_cells = Vec::with_capacity(df.shape().0);
        let mut num_cells_stored_at_resolution = Vec::with_capacity(df.shape().0);
        for (r, n_uncompacted) in df.column("resolution")?.u8()?.into_iter().zip(
            df.column("num_cells_stored_at_resolution")?
                .u64()?
                .into_iter(),
        ) {
            let r = r.unwrap();
            let n_uncompacted = n_uncompacted.unwrap();

            let mut n_stored_compacted = 0u64;
            let mut n_cells_at_resolution = n_uncompacted;
            let mut n_cells = n_uncompacted;
            for (r_c, c_c) in compacted_counts.iter() {
                match r_c.cmp(&r) {
                    Ordering::Less => {
                        n_stored_compacted += c_c;
                        n_cells += c_c * 7u64.pow((r - r_c) as u32);
                    }
                    Ordering::Equal => {
                        n_cells_at_resolution += c_c;
                        n_cells += c_c;
                    }
                    Ordering::Greater => (),
                }
            }
            num_cells_stored_compacted.push(n_stored_compacted);
            num_cells.push(n_cells);
            num_cells_stored_at_resolution.push(n_cells_at_resolution);
        }

        df.with_column(Series::new(
            "num_cells_stored_compacted",
            num_cells_stored_compacted,
        ))?;
        df.with_column(Series::new("num_cells", num_cells))?;
        df.with_column(Series::new(
            "num_cells_stored_at_resolution",
            num_cells_stored_at_resolution,
        ))?;
        df.sort_in_place(["resolution"], vec![false])?;
        Ok(df)
    }
}

fn uncompact(
    h3df: H3DataFrame<H3Cell>,
    cell_subset: Vec<H3Cell>,
    target_resolution: u8,
) -> Result<H3DataFrame<H3Cell>, Error> {
    // use restricted uncompacting to filter by input cells so we
    // avoid over-fetching in case of large, compacted cells.
    let cells: H3CellSet = change_resolution(
        cell_subset
            .into_iter()
            .filter(|c| c.resolution() <= target_resolution),
        target_resolution,
    )
    .filter_map(|c| c.ok())
    .collect();

    h3df.h3_uncompact_dataframe_subset(target_resolution, &cells)
        .map_err(Error::from)
}

fn compacted_counts_stmt(ts: &TableSet, database_name: &str) -> QueryInfo {
    let query = join(
        ts.compacted_tables().into_iter().map(|table| {
            format!(
                "select cast({} as UInt8) as r, count(*) as num_cells_stored_compacted from {}",
                table.spec.h3_resolution,
                table.to_table_name()
            )
        }),
        " union all ",
    );
    QueryInfo {
        query,
        database: database_name.to_string(),
        ..Default::default()
    }
}

fn uncompacted_counts_stmt(ts: &TableSet, database_name: &str) -> QueryInfo {
    let query = join(
        ts.base_tables().into_iter().map(|table| {
            format!(
                "select cast({} as UInt8) as resolution, count(*) as num_cells_stored_at_resolution from {}",
                table.spec.h3_resolution,
                table.to_table_name()
            )
        }),
        " union all ",
    );
    QueryInfo {
        query,
        database: database_name.to_string(),
        ..Default::default()
    }
}

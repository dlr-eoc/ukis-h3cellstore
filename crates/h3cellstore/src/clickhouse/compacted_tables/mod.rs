use std::default::Default;

use arrow_h3::algo::UnCompact;
use async_trait::async_trait;
use tokio::task::spawn_blocking;
use tracing::{info_span, warn, Instrument};

use arrow_h3::h3ron::collections::HashMap;
use arrow_h3::h3ron::H3Cell;
use arrow_h3::H3DataFrame;
use clickhouse_arrow_grpc::{ArrowInterface, QueryInfo};
pub use tableset::{Table, TableSet, TableSpec};

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

/// the column name which must be used for h3indexes.
pub const COL_NAME_H3INDEX: &str = "h3index";

#[async_trait]
pub trait CompactedTablesStore {
    async fn list_tablesets<S>(
        &mut self,
        database_name: S,
    ) -> Result<HashMap<String, TableSet>, Error>
    where
        S: AsRef<str> + Sync + Send;

    async fn drop_tableset<S, TS>(&mut self, database_name: S, tableset: TS) -> Result<(), Error>
    where
        S: AsRef<str> + Send + Sync,
        TS: LoadTableSet + Send + Sync;

    async fn create_tableset_schema<S>(
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
        h3df: H3DataFrame,
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
        query: TableSetQuery,
        cells: Vec<H3Cell>,
        h3_resolution: u8,
    ) -> Result<H3DataFrame, Error>
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

    async fn create_tableset_schema<S>(
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
        h3df: H3DataFrame,
        options: InsertOptions,
    ) -> Result<(), Error>
    where
        S: AsRef<str> + Sync + Send,
    {
        if h3df.dataframe.is_empty() {
            return Ok(());
        }

        let h3df_shape = h3df.dataframe.shape();

        let mut inserter = Inserter::new(
            self.clone(),
            schema.clone(),
            database_name.as_ref().to_string(),
            options,
        );
        let insert_result = inserter
            .insert(h3df)
            .instrument(info_span!(
                "Inserting H3DataFrame into tableset",
                num_rows = h3df_shape.0,
                num_cols = h3df_shape.1,
                schema = schema.name.as_str(),
            ))
            .await;

        // always attempt to cleanup regardless if how the insert went
        let finish_result = inserter
            .finish()
            .instrument(info_span!(
                "Finishing H3DataFrame inserter",
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
        query: TableSetQuery,
        cells: Vec<H3Cell>, // TODO: iterator with borrow and normalize to `h3_resolution`
        h3_resolution: u8,
    ) -> Result<H3DataFrame, Error>
    where
        S: AsRef<str> + Send + Sync,
        TS: LoadTableSet + Send + Sync,
    {
        let tableset = tableset
            .load_tableset_from_store(self, database_name.as_ref())
            .await?;

        let (query_string, cells) = spawn_blocking(move || {
            query
                .build_cell_query_string(&tableset, h3_resolution, &cells)
                .map(|query_string| (query_string, cells))
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

        Ok(spawn_blocking(move || {
            // use restricted uncompacting to filter by input cells so we
            // avoid over-fetching in case of large, compacted cells.
            h3df.uncompact_restricted(h3_resolution, cells)
        })
        .await??)
    }
}

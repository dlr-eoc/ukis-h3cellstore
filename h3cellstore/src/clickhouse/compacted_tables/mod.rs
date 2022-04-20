pub mod schema;

use async_trait::async_trait;
use tracing::warn;

use arrow_h3::h3ron::collections::HashMap;
use clickhouse_arrow_grpc::{ArrowInterface, QueryInfo};

use crate::clickhouse::compacted_tables::schema::CompactedTableSchema;
use crate::clickhouse::tableset::{find_tablesets, TableSet};
use crate::clickhouse::COL_NAME_H3INDEX;
use crate::Error;

#[async_trait]
pub trait CompactedTablesStore {
    async fn list_tablesets<S>(
        &mut self,
        database_name: S,
    ) -> Result<HashMap<String, TableSet>, Error>
    where
        S: AsRef<str> + Sync + Send;

    async fn create_tableset_schema<S>(
        &mut self,
        database_name: S,
        schema: &CompactedTableSchema,
    ) -> Result<(), Error>
    where
        S: AsRef<str> + Sync + Send;
}

#[async_trait]
impl<C> CompactedTablesStore for C
where
    C: ArrowInterface + Send,
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
                        "select table
                from system.columns
                where name = '{}' and database = currentDatabase()",
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
}

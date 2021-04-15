use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use pyo3::exceptions::PyRuntimeError;
use pyo3::PyResult;
use tokio::runtime::{Builder, Runtime};
use tokio::task::JoinHandle as TaskJoinHandle;

use bamboo_h3_int::clickhouse::compacted_tables::{Table, TableSet};
use bamboo_h3_int::clickhouse::query::{
    execute, list_tablesets, query_all, query_all_with_uncompacting, query_returns_rows,
};
use bamboo_h3_int::clickhouse::schema::{CreateSchema, Schema};
use bamboo_h3_int::clickhouse_rs::Pool;

use crate::columnset::ColumnSet;
use crate::error::IntoPyResult;

pub enum Query {
    /// return all rows returned by the given query string
    Plain(String),

    /// return all rows returned by the given query string and perform the uncompacting
    /// client side
    Uncompact(String, HashSet<u64>),
}

/// a synchronous api for the async clickhouse query functions of the _int crate
///
/// Queries are executed in its own thread in a tokio runtime. This means that
/// the CPU-heavier parts of the query functions are also executed within tokio. This
/// leads to tokio being blocked during the CPU-intensive parts, but as the runtime has
/// only very few concurrent tasks it should not matter much.
pub struct ClickhousePool {
    pool: Pool,

    runtime: Runtime,
}

impl ClickhousePool {
    pub fn create(db_url: &str) -> PyResult<ClickhousePool> {
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|e| {
                PyRuntimeError::new_err(format!("Unable to create tokio runtime: {:?}", e))
            })?;

        let pool = Pool::new(db_url);

        // check if the connections are usable
        runtime.block_on(async {
            (&pool)
                .get_handle()
                .await
                .into_pyresult()?
                .check_connection()
                .await
                .into_pyresult()
        })?;

        Ok(Self { pool, runtime })
    }

    pub fn execute(&self, query_string: &str) -> PyResult<()> {
        let p = &self.pool;
        self.runtime.block_on(async {
            let client = p.get_handle().await.into_pyresult()?;
            execute(client, query_string.to_string())
                .await
                .into_pyresult()
        })
    }

    pub fn query(&self, query: Query) -> PyResult<bamboo_h3_int::ColumnSet> {
        let p = &self.pool;
        self.runtime.block_on(async {
            let client = p.get_handle().await.into_pyresult()?;
            match query {
                Query::Plain(query_string) => query_all(client, query_string).await,

                // while it is not great to block tokio with the CPU-heavy uncompacting, it
                // should be ok here, as we do not want to issue too many parallel queries anyways.
                Query::Uncompact(query_string, h3index_set) => {
                    query_all_with_uncompacting(client, query_string, h3index_set).await
                }
            }
            .into_pyresult()
        })
    }

    #[svgbobdoc::transform]
    /// run a query task without waiting for its result. Returns a JoinHandle to
    /// obtain the result later
    ///
    /// The main idea behind is to push queries to background threads to reduce
    /// the time the python thread got to wait for query results
    ///
    /// ´´´svgbob
    /// .---------------------------------.  .-----------------------------.
    /// |   python doing some work, incl. |  |  python obtaining and using |
    /// |  starting the query             |  |  the queryresults           |
    /// `---------------------------------'  `-----------------------------'
    ///                   .--------------------.
    ///                   |  the running query |
    ///                   | incl. uncompacting |
    ///                   `--------------------'
    /// ´´´
    pub fn spawn_query(&self, query_kind: Query) -> TaskJoinHandle<PyResult<ColumnSet>> {
        let p = &self.pool;
        let get_handle = self.runtime.block_on(async { p.get_handle().await });
        self.runtime.spawn(async {
            let client = get_handle.into_pyresult()?;
            match query_kind {
                Query::Plain(query_string) => query_all(client, query_string).await,

                // while it is not great to block tokio with the CPU-heavy uncompacting, it
                // should be ok here, as we do not want to issue too many parallel queries anyways.
                Query::Uncompact(query_string, h3index_set) => {
                    query_all_with_uncompacting(client, query_string, h3index_set).await
                }
            }
            .map(|hm| hm.into())
            .into_pyresult()
        })
    }

    /// obtain the result of a formerly started query task (with `spawn_query`)
    pub fn await_query(
        &self,
        join_handle: TaskJoinHandle<PyResult<ColumnSet>>,
    ) -> PyResult<ColumnSet> {
        self.runtime.block_on(async move {
            join_handle.await.map_err(|e| {
                PyRuntimeError::new_err(format!("could not join awaited query handle: {:?}", e))
            })?
        })
    }

    pub fn query_returns_rows(&self, query_string: String) -> PyResult<bool> {
        let p = &self.pool;
        self.runtime
            .block_on(async {
                let client = p.get_handle().await?;
                query_returns_rows(client, query_string).await
            })
            .into_pyresult()
    }

    pub fn list_tablesets(&self) -> PyResult<HashMap<String, TableSet>> {
        let p = &self.pool;
        self.runtime
            .block_on(async {
                let client = p.get_handle().await?;
                list_tablesets(client).await
            })
            .into_pyresult()
    }

    pub fn drop_tableset(&self, tableset: &TableSet) -> PyResult<()> {
        for tablespec in tableset
            .base_tables
            .values()
            .into_iter()
            .chain(tableset.compacted_tables.values().into_iter())
            // drop starting with the higher resolutions (= larger table size) going to the lower ones
            // to reduce the chance of having partial tablesets when ClickHouses
            // `max_table_size_to_drop` limit kicks in.
            // https://clickhouse.tech/docs/en/operations/server-configuration-parameters/settings/#max-table-size-to-drop
            .sorted_by(|a, b| {
                if a.h3_resolution < b.h3_resolution {
                    Ordering::Greater
                } else if a.h3_resolution > b.h3_resolution || b.is_compacted {
                    // the compacted table is most likely larger than the base table,
                    // so the base table should be dropped first.
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            })
        {
            let table = Table {
                basename: tableset.basename.clone(),
                spec: tablespec.clone(),
            };
            self.execute(&format!("drop table if exists {}", table.to_table_name()))?;
        }
        Ok(())
    }

    pub fn create_schema(&self, schema: &Schema) -> PyResult<()> {
        let mut statements = schema.create_statements().into_pyresult()?;
        let p = &self.pool;
        self.runtime.block_on(async {
            let mut client = p.get_handle().await.into_pyresult()?;
            for s in statements.drain(..) {
                client.execute(s).await.into_pyresult()?
            }
            Ok(())
        })
    }
}

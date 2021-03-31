use std::collections::{HashMap, HashSet};

use pyo3::exceptions::PyRuntimeError;
use pyo3::PyResult;
use tokio::runtime::{Builder, Runtime};
use tokio::task::JoinHandle as TaskJoinHandle;

use crate::convert::ColumnSet;
use crate::error::IntoPyResult;
use bamboo_h3_int::clickhouse::compacted_tables::TableSet;
use bamboo_h3_int::clickhouse::query::{
    list_tablesets, query_all, query_all_with_uncompacting, query_returns_rows,
};
use bamboo_h3_int::clickhouse_rs::Pool;

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

        Ok(Self {
            pool: Pool::new(db_url),
            runtime,
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
}

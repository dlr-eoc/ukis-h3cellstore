use std::collections::{HashMap, HashSet};
use std::thread;
use std::thread::JoinHandle;

use pyo3::{PyErr, PyResult};
use pyo3::exceptions::PyRuntimeError;
use tokio::runtime::{Handle, Runtime};
use tokio::task::JoinHandle as TaskJoinHandle;

use h3cpy_int::clickhouse::query::{
    list_tablesets, query_all, query_all_with_uncompacting, query_returns_rows,
};
use h3cpy_int::clickhouse_rs::{errors::Error as ChError, errors::Result as ChResult, Pool};
use h3cpy_int::ColVec;
use h3cpy_int::compacted_tables::TableSet;

fn ch_to_pyerr(ch_err: ChError) -> PyErr {
    PyRuntimeError::new_err(format!("clickhouse error: {:?}", ch_err))
}

fn ch_to_pyresult<T>(res: ChResult<T>) -> PyResult<T> {
    match res {
        Ok(v) => Ok(v),
        Err(e) => Err(ch_to_pyerr(e)),
    }
}

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

    /// background thread for running the tokio runtime
    tokio_thread: Option<JoinHandle<()>>,
    tokio_handle: Handle,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl ClickhousePool {
    pub fn create(db_url: &str) -> PyResult<ClickhousePool> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let (handle_tx, handle_rx) = std::sync::mpsc::channel();

        let tokio_thread = thread::spawn(move || {
            let mut runtime = Runtime::new().expect("Unable to create tokio runtime");

            // Give a handle to the runtime to another thread.
            handle_tx
                .send(runtime.handle().clone())
                .expect("unable to give runtime handle to another thread");

            // Continue running until notified to shutdown
            runtime.block_on(async {
                shutdown_rx.await.expect("Error on the shutdown channel");
            });
        });

        let tokio_handle = handle_rx.recv().map_err(|e| {
            PyRuntimeError::new_err(format!(
                "Could not get a handle to the other thread's runtime: {:?}",
                e
            ))
        })?;

        Ok(Self {
            pool: Pool::new(db_url),
            tokio_thread: Some(tokio_thread),
            tokio_handle,
            shutdown_tx: Some(shutdown_tx),
        })
    }

    pub fn query(&self, query: Query) -> PyResult<HashMap<String, ColVec>> {
        let p = &self.pool;
        self.tokio_handle.block_on(async {
            let client = match p.get_handle().await {
                Ok(c) => c,
                Err(e) => return Err(ch_to_pyerr(e)),
            };
            let res = match query {
                Query::Plain(query_string) => query_all(client, query_string).await,

                // while it is not great to block tokio with the CPU-heavy uncompacting, it
                // should be ok here, as we do not want to issue too many parallel queries anyways.
                Query::Uncompact(query_string, h3index_set) => {
                    query_all_with_uncompacting(client, query_string, h3index_set).await
                }
            };
            ch_to_pyresult(res)
        })
    }

    /// run a query task without waiting for its result. Returns a joinHandle to
    /// obtain the result later
    pub fn spawn_query(
        &self,
        query_kind: Query,
    ) -> TaskJoinHandle<PyResult<HashMap<String, ColVec>>> {
        let p = &self.pool;
        let gethandle = self.tokio_handle.block_on(async { p.get_handle().await });
        self.tokio_handle.spawn(async {
            let client = match gethandle {
                Ok(c) => c,
                Err(e) => return Err(ch_to_pyerr(e)),
            };
            let res = match query_kind {
                Query::Plain(query_string) => query_all(client, query_string).await,

                // while it is not great to block tokio with the CPU-heavy uncompacting, it
                // should be ok here, as we do not want to issue too many parallel queries anyways.
                Query::Uncompact(query_string, h3index_set) => {
                    query_all_with_uncompacting(client, query_string, h3index_set).await
                }
            };
            ch_to_pyresult(res)
        })
    }

    /// obtain the result of a formerly started query task (with `spawn_query`)
    pub fn await_query(
        &self,
        join_handle: TaskJoinHandle<PyResult<HashMap<String, ColVec>>>,
    ) -> PyResult<HashMap<String, ColVec>> {
        self.tokio_handle.block_on(async move {
            join_handle.await.map_err(|e| {
                PyRuntimeError::new_err(format!("could not join awaited query handle: {:?}", e))
            })?
        })
    }

    pub fn query_returns_rows(&self, query_string: String) -> PyResult<bool> {
        let p = &self.pool;
        ch_to_pyresult(self.tokio_handle.block_on(async {
            let client = p.get_handle().await?;
            query_returns_rows(client, query_string).await
        }))
    }

    pub fn query_all_with_uncompacting(
        &self,
        query_string: String,
        h3index_set: HashSet<u64>,
    ) -> PyResult<HashMap<String, ColVec>> {
        let p = &self.pool;
        ch_to_pyresult(self.tokio_handle.block_on(async {
            let client = p.get_handle().await?;
            query_all_with_uncompacting(client, query_string, h3index_set).await
        }))
    }

    pub fn list_tablesets(&self) -> PyResult<HashMap<String, TableSet>> {
        let p = &self.pool;
        ch_to_pyresult(self.tokio_handle.block_on(async {
            let client = p.get_handle().await?;
            list_tablesets(client).await
        }))
    }
}

impl Drop for ClickhousePool {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            shutdown_tx
                .send(())
                .expect("Unable to shutdown tokio runtime thread");
        }
        if let Some(tokio_thread) = self.tokio_thread.take() {
            tokio_thread.join().expect("tokio thread panicked");
        }
    }
}

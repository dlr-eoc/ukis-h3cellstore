use crate::error::IntoPyResult;
use h3cellstore::clickhouse::H3CellStore;
use h3cellstore::export::clickhouse_arrow_grpc::export::tonic::transport::Channel;
use h3cellstore::export::clickhouse_arrow_grpc::{ArrowInterface, ClickHouseClient, QueryInfo};
use pyo3::exceptions::{PyIOError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::PyResult;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};
use tracing::debug_span;

#[pyclass]
pub struct GRPCRuntime {
    runtime: Arc<Runtime>,
}

#[pymethods]
impl GRPCRuntime {
    #[new]
    pub fn new(num_worker_threads: usize) -> PyResult<Self> {
        let span = debug_span!(
            "Creating tokio runtime",
            num_worker_threads = num_worker_threads
        );
        let _ = span.enter();

        let runtime = Builder::new_multi_thread()
            .worker_threads(num_worker_threads)
            .enable_all()
            .build()
            .map_err(|e| {
                PyRuntimeError::new_err(format!("Unable to create tokio runtime: {:?}", e))
            })?;
        Ok(Self {
            runtime: Arc::new(runtime),
        })
    }
}

#[pyclass]
pub struct GRPCConnection {
    database_name: String,
    runtime: Arc<Runtime>,
    client: ClickHouseClient<Channel>,
}

#[pymethods]
impl GRPCConnection {
    #[staticmethod]
    pub fn connect(
        grpc_endpoint: &str,
        database_name: &str,
        create_db: bool,
        grpc_runtime: &GRPCRuntime,
    ) -> PyResult<Self> {
        let runtime = grpc_runtime.runtime.clone();
        let grpc_endpoint_str = grpc_endpoint.to_string();
        let db_name_str = database_name.to_string();
        let client =
            runtime.block_on(async { connect(grpc_endpoint_str, db_name_str, create_db).await })?;

        Ok(Self {
            database_name: database_name.to_string(),
            runtime,
            client,
        })
    }
}

async fn connect(
    grpc_endpoint: String,
    database_name: String,
    create_db: bool,
) -> PyResult<ClickHouseClient<Channel>> {
    let mut client = ClickHouseClient::connect(grpc_endpoint)
        .await
        .into_pyresult()?
        .send_gzip()
        .accept_gzip();

    if create_db {
        client
            .execute_query_checked(QueryInfo {
                query: format!("create database if not exists {}", database_name),
                database: "system".to_string(),
                ..Default::default()
            })
            .await
            .into_pyresult()?;
    }

    // check if db exists
    if !client
        .database_exists(&database_name)
        .await
        .into_pyresult()?
    {
        return Err(PyIOError::new_err(format!(
            "database {} does not exist",
            database_name
        )));
    }
    Ok(client)
}

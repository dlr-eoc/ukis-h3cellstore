use crate::clickhouse::schema::PyCompactedTableSchema;
use crate::clickhouse::tableset::PyTableSet;
use crate::error::IntoPyResult;
use crate::frame::wrapped_frame;
use crate::{PyDataFrame, PyH3DataFrame};
use h3cellstore::clickhouse::compacted_tables::CompactedTablesStore;
use h3cellstore::clickhouse::H3CellStore;
use h3cellstore::export::clickhouse_arrow_grpc::export::tonic::transport::Channel;
use h3cellstore::export::clickhouse_arrow_grpc::{ArrowInterface, ClickHouseClient, QueryInfo};
use pyo3::exceptions::{PyIOError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::PyResult;
use std::collections::HashMap;
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

    /// execute the given query in the database without returning any result
    pub fn execute(&mut self, query: String) -> PyResult<()> {
        self.runtime
            .block_on(async {
                self.client
                    .execute_query_checked(QueryInfo {
                        query,
                        database: self.database_name.clone(),
                        ..Default::default()
                    })
                    .await
            })
            .into_pyresult()
            .map(|_| ())
    }

    /// execute the given query and return a non-H3 dataframe of it
    pub fn execute_into_dataframe(&mut self, py: Python, query: String) -> PyResult<PyObject> {
        let df: PyDataFrame = self
            .runtime
            .block_on(async {
                self.client
                    .execute_into_dataframe(QueryInfo {
                        query,
                        database: self.database_name.clone(),
                        ..Default::default()
                    })
                    .await
            })
            .into_pyresult()?
            .into();
        wrapped_frame(py, df)
    }

    /// execute the given query and return a H3 dataframe of it
    pub fn execute_into_h3dataframe(
        &mut self,
        py: Python,
        query: String,
        h3index_column_name: String,
    ) -> PyResult<PyObject> {
        let df: PyH3DataFrame = self
            .runtime
            .block_on(async {
                self.client
                    .execute_into_h3dataframe(
                        QueryInfo {
                            query,
                            database: self.database_name.clone(),
                            ..Default::default()
                        },
                        h3index_column_name,
                    )
                    .await
            })
            .into_pyresult()?
            .into();
        wrapped_frame(py, df)
    }

    /// Check if the given DB exists
    pub fn database_exists(&mut self, database_name: String) -> PyResult<bool> {
        self.runtime
            .block_on(async { self.client.database_exists(database_name).await })
            .into_pyresult()
    }

    /// list the tablesets found it the current database
    pub fn list_tablesets(&mut self) -> PyResult<HashMap<String, PyTableSet>> {
        Ok(self
            .runtime
            .block_on(async { self.client.list_tablesets(&self.database_name).await })
            .into_pyresult()?
            .drain()
            .map(|(name, tableset)| (name, tableset.into()))
            .collect())
    }

    /// drop the tableset with the given name
    pub fn drop_tableset(&mut self, tableset_name: String) -> PyResult<()> {
        self.runtime
            .block_on(async {
                self.client
                    .drop_tableset(&self.database_name, tableset_name)
                    .await
            })
            .into_pyresult()
    }

    /// create the schema based on the schema definition in the database
    pub fn create_tableset_schema(&mut self, schema: &PyCompactedTableSchema) -> PyResult<()> {
        self.runtime
            .block_on(async {
                self.client
                    .create_tableset_schema(&self.database_name, &schema.schema)
                    .await
            })
            .into_pyresult()
    }

    /// deduplicate the contents of the given database schema
    pub fn deduplicate_schema(&mut self, schema: &PyCompactedTableSchema) -> PyResult<()> {
        self.runtime
            .block_on(async {
                self.client
                    .deduplicate_schema(&self.database_name, &schema.schema)
                    .await
            })
            .into_pyresult()
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

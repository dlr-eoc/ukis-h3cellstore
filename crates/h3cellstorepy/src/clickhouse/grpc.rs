use crate::clickhouse::schema::PyCompactedTableSchema;
use crate::clickhouse::tableset::PyTableSet;
use crate::error::IntoPyResult;
use crate::frame::{dataframe_from_pyany, wrapped_frame};
use crate::utils::cells_from_numpy;
use crate::{PyDataFrame, PyH3DataFrame};
use h3cellstore::clickhouse::compacted_tables::{
    CompactedTablesStore, InsertOptions, TableSetQuery,
};
use h3cellstore::clickhouse::H3CellStore;
use h3cellstore::export::arrow_h3::H3DataFrame;
use h3cellstore::export::clickhouse_arrow_grpc::export::tonic::transport::Channel;
use h3cellstore::export::clickhouse_arrow_grpc::{ArrowInterface, ClickHouseClient, QueryInfo};
use numpy::PyReadonlyArray1;
use pyo3::exceptions::{PyIOError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::PyResult;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::oneshot::error::TryRecvError;
use tracing::debug_span;
use tracing::log::warn;

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

    /// insert a dataframe into a table
    pub fn insert_dataframe(
        &mut self,
        py: Python,
        table_name: String,
        dataframe: &PyAny,
    ) -> PyResult<()> {
        let df = dataframe_from_pyany(py, dataframe)?;
        self.runtime
            .block_on(async {
                self.client
                    .insert_dataframe(&self.database_name, table_name, df)
                    .await
            })
            .into_pyresult()
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
    pub fn create_tableset(&mut self, schema: &PyCompactedTableSchema) -> PyResult<()> {
        self.runtime
            .block_on(async {
                self.client
                    .create_tableset(&self.database_name, &schema.schema)
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

    /// insert a dataframe into a tableset
    ///#[args(options=None)]
    pub fn insert_h3dataframe_into_tableset(
        &self,
        py: Python,
        schema: &PyCompactedTableSchema,
        dataframe: &PyAny,
        options: Option<&PyInsertOptions>,
    ) -> PyResult<()> {
        let insert_options = options.map(|o| o.options.clone()).unwrap_or_default();
        let h3df: H3DataFrame = (
            dataframe_from_pyany(py, dataframe)?,
            schema.schema.h3index_column().into_pyresult()?.0.clone(),
        )
            .try_into()
            .into_pyresult()?;

        let abort_mutex = insert_options.abort.clone();
        let (oneshot_send, mut oneshot_recv) = tokio::sync::oneshot::channel();

        let database_name = self.database_name.clone();
        let mut client = self.client.clone();
        let schema = schema.schema.clone();
        let joinhandle = self.runtime.spawn(async move {
            let res = client
                .insert_h3dataframe_into_tableset(database_name, &schema, h3df, insert_options)
                .await
                .into_pyresult();

            oneshot_send.send(res).expect("sending over channel failed")
        });

        loop {
            if py.check_signals().is_err() {
                if let Ok(mut guard) = abort_mutex.lock() {
                    warn!("Received Abort-request during insert");
                    *guard = true;
                }
            }

            match oneshot_recv.try_recv() {
                Ok(res) => {
                    self.runtime
                        .block_on(async { joinhandle.await })
                        .into_pyresult()?;
                    return res;
                }
                Err(TryRecvError::Empty) => std::thread::sleep(Duration::from_millis(50)),
                Err(TryRecvError::Closed) => unreachable!(),
            }
        }
    }

    pub fn query_tableset_cells(
        &mut self,
        py: Python,
        tableset_name: String,
        query: &PyTableSetQuery,
        cells: PyReadonlyArray1<u64>,
        h3_resolution: u8,
    ) -> PyResult<PyObject> {
        let cells = cells_from_numpy(cells)?;
        let query = query.query.clone();
        let h3df: PyH3DataFrame = self
            .runtime
            .block_on(async {
                self.client
                    .query_tableset_cells(
                        &self.database_name,
                        tableset_name,
                        query,
                        cells,
                        h3_resolution,
                    )
                    .await
            })
            .into_pyresult()?
            .into();

        wrapped_frame(py, h3df)
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

#[pyclass]
pub struct PyInsertOptions {
    options: InsertOptions,
}

#[pymethods]
impl PyInsertOptions {
    #[new]
    fn new() -> Self {
        Self {
            options: Default::default(),
        }
    }

    #[getter]
    fn get_max_num_rows_per_chunk(&self) -> usize {
        self.options.max_num_rows_per_chunk
    }

    #[setter]
    fn set_max_num_rows_per_chunk(&mut self, max_num_rows_per_chunk: usize) {
        self.options.max_num_rows_per_chunk = max_num_rows_per_chunk
    }

    #[getter]
    fn get_create_schema(&self) -> bool {
        self.options.create_schema
    }

    #[setter]
    fn set_create_schema(&mut self, create_schema: bool) {
        self.options.create_schema = create_schema
    }

    #[getter]
    fn get_deduplicate_after_insert(&self) -> bool {
        self.options.deduplicate_after_insert
    }

    #[setter]
    fn set_deduplicate_after_insert(&mut self, deduplicate_after_insert: bool) {
        self.options.deduplicate_after_insert = deduplicate_after_insert
    }
}

#[pyclass]
pub struct PyTableSetQuery {
    query: TableSetQuery,
}

#[pymethods]
impl PyTableSetQuery {
    // todo: templated query

    #[new]
    fn new() -> Self {
        Self {
            query: TableSetQuery::AutoGenerated,
        }
    }
}

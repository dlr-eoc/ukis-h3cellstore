use crate::clickhouse::schema::PyCompactedTableSchema;
use crate::clickhouse::tableset::PyTableSet;
use crate::clickhouse::traversal::{PyTraversalOptions, PyTraverser};
use crate::error::IntoPyResult;
use crate::frame::{dataframe_from_pyany, ToDataframeWrapper};
use crate::utils::indexes_from_numpy;
use h3cellstore::clickhouse::compacted_tables::{
    CompactedTablesStore, InsertOptions, QueryOptions, TableSetQuery,
};
use h3cellstore::clickhouse::H3CellStore;
use h3cellstore::export::clickhouse_arrow_grpc::{ArrowInterface, Client, QueryInfo};
use h3cellstore::export::h3ron_polars::frame::H3DataFrame;
use numpy::PyReadonlyArray1;
use pyo3::exceptions::{PyIOError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::PyResult;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::oneshot::error::TryRecvError;
use tracing::debug_span;
use tracing::log::warn;

#[derive(Clone)]
#[pyclass]
pub struct GRPCRuntime {
    runtime: Arc<Runtime>,
}

#[pymethods]
impl GRPCRuntime {
    #[new]
    #[args(num_worker_threads = "None")]
    pub fn new(num_worker_threads: Option<usize>) -> PyResult<Self> {
        let span = debug_span!(
            "Creating tokio runtime",
            num_worker_threads = num_worker_threads
        );
        let _ = span.enter();

        let mut builder = Builder::new_multi_thread();

        if let Some(nwt) = num_worker_threads {
            builder.worker_threads(nwt);
        }

        let runtime = builder.enable_all().build().map_err(|e| {
            PyRuntimeError::new_err(format!("Unable to create tokio runtime: {:?}", e))
        })?;
        Ok(Self {
            runtime: Arc::new(runtime),
        })
    }
}

/// obtain the runtime defined in the python module
fn obtain_runtime() -> PyResult<Arc<Runtime>> {
    // load the default runtime created by the python module.
    Python::with_gil(|py| {
        let module = py.import(concat!(env!("CARGO_PKG_NAME"), ".clickhouse"))?;

        let runtime = module
            .getattr("_RUNTIME")?
            .extract::<PyRef<'_, GRPCRuntime>>()?
            .runtime
            .clone();
        Ok(runtime)
    })
}

/// GPRC connection to the Clickhouse DB server.
///
/// Uses async communication using a internal tokio runtime.
#[pyclass]
pub struct GRPCConnection {
    pub(crate) database_name: String,
    pub(crate) runtime: Arc<Runtime>,
    pub(crate) client: Client,
}

#[pymethods]
impl GRPCConnection {
    /// Name of the DB the connection connects to
    #[getter]
    fn database_name(&self) -> &str {
        self.database_name.as_str()
    }

    /// Establish a new connection
    #[args(create_db = "false", runtime = "None")]
    #[new]
    pub fn new(
        grpc_endpoint: &str,
        database_name: &str,
        create_db: bool,
        runtime: Option<GRPCRuntime>,
    ) -> PyResult<Self> {
        let runtime = match runtime {
            None => obtain_runtime()?,
            Some(gprc_runtime) => gprc_runtime.runtime,
        };
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
    pub fn execute_into_dataframe(&mut self, query: String) -> PyResult<PyObject> {
        self.runtime
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
            .to_dataframewrapper()
    }

    /// insert a dataframe into a table
    pub fn insert_dataframe(&mut self, table_name: String, dataframe: &PyAny) -> PyResult<()> {
        let df = dataframe_from_pyany(dataframe)?;
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
        query: String,
        h3index_column_name: String,
    ) -> PyResult<PyObject> {
        self.runtime
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
            .to_dataframewrapper()
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
    pub fn insert_h3dataframe_into_tableset(
        &self,
        schema: &PyCompactedTableSchema,
        dataframe: &PyAny,
        options: Option<&PyInsertOptions>,
    ) -> PyResult<()> {
        let insert_options = options.map(|o| o.options.clone()).unwrap_or_default();
        let h3df = H3DataFrame::from_dataframe(
            dataframe_from_pyany(dataframe)?,
            schema.schema.h3index_column().into_pyresult()?.0,
        )
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
            Python::with_gil(|py| {
                if py.check_signals().is_err() {
                    if let Ok(mut guard) = abort_mutex.lock() {
                        warn!("Received Abort request during insert");
                        *guard = true;
                    }
                }
            });

            match oneshot_recv.try_recv() {
                Ok(res) => {
                    self.runtime
                        .block_on(async { joinhandle.await })
                        .into_pyresult()?;
                    return res;
                }
                Err(TryRecvError::Empty) => std::thread::sleep(Duration::from_millis(100)),
                Err(TryRecvError::Closed) => unreachable!(),
            }
        }
    }

    #[args(do_uncompact = "true")]
    pub fn query_tableset_cells(
        &mut self,
        tableset_name: String,
        query: &PyTableSetQuery,
        cells: PyReadonlyArray1<u64>,
        h3_resolution: u8,
        do_uncompact: bool,
    ) -> PyResult<PyObject> {
        let mut query_options = QueryOptions::new(
            query.query.clone(),
            indexes_from_numpy(cells)?,
            h3_resolution,
        );
        query_options.do_uncompact = do_uncompact;
        self.runtime
            .block_on(async {
                self.client
                    .query_tableset_cells(&self.database_name, tableset_name, query_options)
                    .await
            })
            .into_pyresult()?
            .to_dataframewrapper()
    }

    /// Traversal using multiple GRPC connections with pre-loading in the background without blocking
    /// the python interpreter.
    ///
    /// The `area_of_interest` can be provided in multiple forms:
    ///
    /// - As a geometry or other object implementing pythons `__geo_interface__`. For example created by the `shapely` or `geojson` libraries.
    /// - As a `numpy` array of H3 cells. These will be transformed to a resolution suitable for traversal. See the `max_fetch_count` argument
    ///
    /// Options (provided as keyword arguments):
    ///
    /// - `max_fetch_count`: The maximum number of cells to fetch in one DB query.
    /// - `num_connections`: Number of parallel DB connections to use in the background. Default is 3. Depending with the number of connections used the amount of memory used increases as well as the load put onto the DB-Server. The benefit is getting data faster as it is pre-loaded in the background.
    /// - `filter_query`: This query will be applied to the tables in the reduced `traversal_h3_resolution` and only cells found by this query will be loaded from the tables in the requested full resolution
    #[args(kwargs = "**")]
    pub fn traverse_tableset_area_of_interest(
        &mut self,
        tableset_name: String,
        query: &PyTableSetQuery,
        area_of_interest: &PyAny,
        h3_resolution: u8,
        kwargs: Option<&PyDict>,
    ) -> PyResult<PyTraverser> {
        let options = PyTraversalOptions::extract(kwargs)?;
        PyTraverser::create(
            self,
            tableset_name,
            query.query.clone(),
            area_of_interest,
            h3_resolution,
            options,
        )
    }

    /// get stats about the number of cells and compacted cells in all the
    /// resolutions of the tableset
    pub fn tableset_stats(&mut self, tableset_name: String) -> PyResult<PyObject> {
        self.runtime
            .block_on(async {
                self.client
                    .tableset_stats(&self.database_name, tableset_name)
                    .await
            })
            .into_pyresult()?
            .to_dataframewrapper()
    }
}

async fn connect(
    grpc_endpoint: String,
    database_name: String,
    create_db: bool,
) -> PyResult<Client> {
    let mut client = Client::connect(grpc_endpoint).await.into_pyresult()?;

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
    pub(crate) query: TableSetQuery,
}

#[pymethods]
impl PyTableSetQuery {
    #[new]
    fn new() -> Self {
        Self {
            query: TableSetQuery::AutoGenerated,
        }
    }

    #[staticmethod]
    fn from_template(query_template: String) -> Self {
        Self {
            query: TableSetQuery::TemplatedSelect(query_template),
        }
    }
}

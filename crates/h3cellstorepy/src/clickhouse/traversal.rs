use h3cellstore::clickhouse::compacted_tables::{CompactedTablesStore, TableSet, TableSetQuery};
use h3cellstore::export::arrow_h3::algo::ToIndexCollection;
use numpy::{PyArray1, PyReadonlyArray1};
use postage::prelude::{Sink, Stream};
use py_geo_interface::GeoInterface;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;
use tokio::task::spawn_blocking;
use tokio::time::timeout;
use tracing::log::{debug, info, warn};
use tracing::{debug_span, Instrument};

use crate::clickhouse::grpc::{GRPCConnection, PyTableSetQuery};
use crate::error::IntoPyResult;
use crate::frame::ToDataframeWrapper;
use h3cellstore::export::arrow_h3::export::h3ron::iter::change_resolution;
use h3cellstore::export::arrow_h3::export::h3ron::{H3Cell, ToH3Cells};
use h3cellstore::export::arrow_h3::H3DataFrame;
use h3cellstore::export::clickhouse_arrow_grpc::export::tonic::transport::Channel;
use h3cellstore::export::clickhouse_arrow_grpc::ClickHouseClient;

use crate::utils::{extract_dict_item_option, indexes_from_numpy};

/// find the resolution generate coarser h3 cells to access the tableset without needing to fetch more
/// than `max_fetch_count` indexes per batch.
///
/// That resolution must be a base resolution
fn select_traversal_resolution(
    tableset: &TableSet,
    target_h3_resolution: u8,
    max_fetch_count: usize,
) -> u8 {
    let mut resolutions: Vec<_> = tableset
        .base_resolutions()
        .iter()
        .filter(|r| **r < target_h3_resolution)
        .copied()
        .collect();
    resolutions.sort_unstable();

    let mut traversal_resolution = target_h3_resolution;
    for r in resolutions {
        let r_diff = (target_h3_resolution - r) as u32;
        if 7_u64.pow(r_diff) <= (max_fetch_count as u64) {
            traversal_resolution = r;
            break;
        }
    }
    if (target_h3_resolution as i16 - traversal_resolution as i16).abs() <= 3 {
        warn!(
            "traversal: using H3 res {} as batch resolution to iterate over H3 res {} data. This is probably inefficient - try to increase max_fetch_num.",
            traversal_resolution,
            target_h3_resolution
        );
    } else {
        info!(
            "traversal: using H3 res {} as traversal_resolution",
            traversal_resolution
        );
    }
    traversal_resolution
}

pub struct TraversalOptions {
    /// The maximum number of cells to fetch in one DB query.
    max_fetch_count: usize,

    /// Number of parallel DB connections to use in the background.
    /// Depending with the number of connections used the amount of memory used increases as well as
    /// the load put onto the DB-Server. The benefit is getting data faster as it is pre-loaded in the
    /// background.
    num_connections: usize,

    /// optional prefilter query.
    ///
    /// This query will be applied to the tables in the reduced `traversal_h3_resolution` and only cells
    /// found by this query will be loaded from the tables in the requested full resolution
    filter_query: Option<TableSetQuery>,
}

impl Default for TraversalOptions {
    fn default() -> Self {
        Self {
            max_fetch_count: 10_000,
            num_connections: 3,
            filter_query: None,
        }
    }
}

impl TraversalOptions {
    pub(crate) fn extract(dict: Option<&PyDict>) -> PyResult<Self> {
        let mut kwargs = Self::default();
        if let Some(dict) = dict {
            if let Some(mfc) = extract_dict_item_option(dict, "max_fetch_count")? {
                kwargs.max_fetch_count = mfc;
            }
            if let Some(nc) = extract_dict_item_option(dict, "num_connections")? {
                kwargs.num_connections = nc;
            }
            if let Some(fq) =
                extract_dict_item_option::<PyRef<'_, PyTableSetQuery>, _>(dict, "filter_query")?
            {
                kwargs.filter_query = Some(fq.query.clone());
            }
        }
        Ok(kwargs)
    }
}

///
/// This class is an iterator over the dataframes encountered during traversal of the `area_of_interest`.
#[pyclass]
pub struct PyTraverser {
    num_traversal_cells: usize,
    traversal_h3_resolution: u8,
    dataframe_recv: Arc<Mutex<tokio::sync::mpsc::Receiver<PyResult<H3DataFrame>>>>,
    runtime: Arc<Runtime>,
}

#[pymethods]
impl PyTraverser {
    /// Number of cells used for the traversal process. Each one of these cells
    /// will be queried using a separate DB query.
    #[getter]
    fn num_traversal_cells(&self) -> usize {
        self.num_traversal_cells
    }

    fn __len__(&self) -> usize {
        self.num_traversal_cells
    }

    /// The H3 resolution used for the traversal process
    #[getter]
    fn traversal_h3_resolution(&self) -> u8 {
        self.traversal_h3_resolution
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(slf: PyRefMut<'_, Self>) -> PyResult<Option<PyObject>> {
        debug!("waiting to receive dataframe of traversed cell");
        loop {
            let recv = slf.dataframe_recv.clone();
            match slf.runtime.block_on(async {
                let mut guard = recv.lock().await;
                timeout(Duration::from_millis(500), guard.recv()).await
            }) {
                Ok(Some(h3df_result)) => {
                    // channel had a waiting message
                    return Ok(Some(h3df_result?.to_dataframewrapper()?));
                }
                Ok(None) => {
                    // channel has been closed - no messages left
                    return Ok(None);
                }
                Err(_) => {
                    // timeout has elapsed - so lets check for user interrupts
                    Python::acquire_gil().python().check_signals()?;
                }
            }
        }
    }
}

impl PyTraverser {
    pub fn create(
        conn: &mut GRPCConnection,
        tableset_name: String,
        query: TableSetQuery,
        area_of_interest: &PyAny,
        h3_resolution: u8,
        options: TraversalOptions,
    ) -> PyResult<Self> {
        let tableset = conn
            .runtime
            .block_on(async {
                conn.client
                    .get_tableset(conn.database_name.as_str(), tableset_name)
                    .await
            })
            .into_pyresult()?;
        let traversal_h3_resolution =
            select_traversal_resolution(&tableset, h3_resolution, options.max_fetch_count);
        let traversal_cells = area_of_interest_cells(area_of_interest, traversal_h3_resolution)?;
        let num_traversal_cells = traversal_cells.len();
        let runtime = conn.runtime.clone();
        let mut context = WorkerContext {
            client: conn.client.clone(),
            database_name: conn.database_name.clone(),
            tableset,
        };
        let (dataframe_send, dataframe_recv) = tokio::sync::mpsc::channel(options.num_connections);

        let _background_fetch = runtime.spawn(async move {
            let (mut trav_cells_send, _trav_cells_recv) =
                postage::dispatch::channel(2 * options.num_connections);

            // spawn the workers performing the db-work
            for _ in 0..(options.num_connections) {
                let mut worker_context = context.clone();
                let mut worker_trav_cells_recv = trav_cells_send.subscribe();
                let worker_dataframe_send = dataframe_send.clone();
                let worker_query = query.clone();

                tokio::task::spawn(async move {
                    while let Some(cell) = worker_trav_cells_recv.recv().await {
                        let message = match load_traversed_cell(
                            &mut worker_context,
                            worker_query.clone(),
                            cell,
                            h3_resolution,
                        )
                        .await
                        {
                            Ok(Some(h3df)) => Ok(h3df),
                            Ok(None) => {
                                // no data found, continue to the next cell
                                info!("traversal cell yielded no data - skipping");
                                continue;
                            }
                            Err(e) => Err(e),
                        };

                        if worker_dataframe_send.send(message).await.is_err() {
                            debug!("worker channel has been closed upstream. shutting down worker");
                            break;
                        } else {
                            info!("traversal cell loaded and send");
                        }
                    }
                });
            }

            // distribute the cells to the workers
            let _ = tokio::task::spawn(async move {
                if let Some(filter_query) = &options.filter_query {
                    for cell_chunk in traversal_cells.chunks(50) {
                        dispatch_traversal_cells(
                            &mut trav_cells_send,
                            prefilter_traversal_cells(
                                &mut context,
                                filter_query.clone(),
                                cell_chunk,
                                traversal_h3_resolution,
                            )
                            .await,
                        )
                        .await;
                    }
                } else {
                    dispatch_traversal_cells(&mut trav_cells_send, Ok(traversal_cells)).await;
                }
            });
        });

        // end of this scope closes the local copy of the dataframe_send channel to allow the
        // pipeline to collapse when ta traversal is finished

        Ok(Self {
            num_traversal_cells,
            traversal_h3_resolution,
            dataframe_recv: Arc::new(Mutex::new(dataframe_recv)),
            runtime,
        })
    }
}

#[derive(Clone)]
struct WorkerContext {
    client: ClickHouseClient<Channel>,
    database_name: String,
    tableset: TableSet,
}

///
///
/// The cells are returned sorted for a deterministic traversal order
fn area_of_interest_cells(
    area_of_interest: &PyAny,
    traversal_resolution: u8,
) -> PyResult<Vec<H3Cell>> {
    if let Ok(geointerface) = GeoInterface::extract(area_of_interest) {
        let mut cells: Vec<_> = geointerface
            .0
            .to_h3_cells(traversal_resolution)
            .into_pyresult()?
            .iter()
            .collect();
        cells.sort_unstable();
        Ok(cells)
    } else if area_of_interest.is_instance_of::<PyArray1<u64>>()? {
        let validated_cells: Vec<H3Cell> =
            indexes_from_numpy(area_of_interest.extract::<PyReadonlyArray1<u64>>()?)?;

        let mut traversal_cells = change_resolution(validated_cells, traversal_resolution)
            .collect::<Result<Vec<_>, _>>()
            .into_pyresult()?;

        traversal_cells.sort_unstable();
        traversal_cells.dedup();
        Ok(traversal_cells)
    } else {
        Err(PyValueError::new_err(
            "unsupported type for area_of_interest",
        ))
    }
}

async fn dispatch_traversal_cells(
    sender: &mut postage::dispatch::Sender<PyResult<H3Cell>>,
    traversal_cells: PyResult<Vec<H3Cell>>,
) {
    match traversal_cells {
        Ok(cells) => {
            for cell in cells {
                if sender.send(Ok(cell)).await.is_err() {
                    debug!("sink rejected message");
                    break;
                }
            }
        }
        Err(e) => {
            if sender.send(Err(e)).await.is_err() {
                debug!("sink rejected message");
            }
        }
    }
}

async fn prefilter_traversal_cells(
    worker_context: &mut WorkerContext,
    filter_query: TableSetQuery,
    cells: &[H3Cell],
    traversal_h3_resolution: u8,
) -> PyResult<Vec<H3Cell>> {
    if cells.is_empty() {
        return Ok(vec![]);
    }

    let filter_h3df = worker_context
        .client
        .query_tableset_cells(
            &worker_context.database_name,
            worker_context.tableset.clone(),
            filter_query,
            cells.to_vec(),
            traversal_h3_resolution,
        )
        .await
        .into_pyresult()?;

    // use only the indexes from the filter query to be able to fetch a smaller subset
    spawn_blocking(move || {
        filter_h3df
            .to_index_collection()
            .into_pyresult()
            .map(|mut cells: Vec<H3Cell>| {
                // remove duplicates
                cells.sort_unstable();
                cells.dedup();
                cells
            })
    })
    .await
    .into_pyresult()?
}

async fn load_traversed_cell(
    worker_context: &mut WorkerContext,
    query: TableSetQuery,
    cell: PyResult<H3Cell>,
    h3_resolution: u8,
) -> PyResult<Option<H3DataFrame>> {
    match cell {
        Ok(cell) => {
            let h3df = worker_context
                .client
                .query_tableset_cells(
                    &worker_context.database_name,
                    worker_context.tableset.clone(),
                    query,
                    vec![cell],
                    h3_resolution,
                )
                .instrument(debug_span!(
                    "Loading traversal cell",
                    cell = cell.to_string().as_str()
                ))
                .await
                .into_pyresult()?;

            if h3df.dataframe.shape().0 == 0 {
                // no data found, continue to the next cell
                info!("Discarding received empty dataframe");
                return Ok(None);
            }
            Ok(Some(h3df))
        }
        Err(e) => Err(e),
    }
}

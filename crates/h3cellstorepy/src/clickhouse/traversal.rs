use futures::StreamExt;
use h3cellstore::clickhouse::compacted_tables::traversal::{
    traverse, TraversalArea, TraversalOptions, Traverser,
};
use h3cellstore::clickhouse::compacted_tables::TableSetQuery;
use numpy::{PyArray1, PyReadonlyArray1};
use py_geo_interface::GeoInterface;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing::log::debug;

use crate::clickhouse::grpc::{GRPCConnection, PyTableSetQuery};
use crate::error::IntoPyResult;
use crate::frame::ToDataframeWrapper;
use h3cellstore::export::arrow_h3::export::h3ron::H3Cell;

use crate::utils::{extract_dict_item_option, indexes_from_numpy};

pub struct PyTraversalOptions {
    /// The maximum number of cells to fetch in one DB query.
    ///
    /// Please note that this setting controls only the number of cells
    /// requested from the DB. Should - for example - each cell have data
    /// for multiple time steps in the database, more rows will be returned.
    ///
    /// This setting is crucial to control the size of the messages transferred from
    /// Clickhouse. So, decrease when Clickhouse runs into GRPC message size limits
    /// (protobuf supports max. 2GB).
    max_h3indexes_fetch_count: usize,

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

    /// uncompact the cells loaded from the db. This should be true in most cases.
    do_uncompact: bool,
}

impl Default for PyTraversalOptions {
    fn default() -> Self {
        let upstream_defaults = TraversalOptions::default();
        Self {
            max_h3indexes_fetch_count: upstream_defaults.max_h3indexes_fetch_count,
            num_connections: upstream_defaults.num_connections,
            filter_query: upstream_defaults.filter_query,
            do_uncompact: upstream_defaults.do_uncompact,
        }
    }
}

impl PyTraversalOptions {
    pub(crate) fn extract(dict: Option<&PyDict>) -> PyResult<Self> {
        let mut kwargs = Self::default();
        if let Some(dict) = dict {
            if let Some(mfc) = extract_dict_item_option(dict, "max_h3indexes_fetch_count")? {
                kwargs.max_h3indexes_fetch_count = mfc;
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
    traverser: Arc<Mutex<Traverser>>,
    runtime: Arc<Runtime>,
}

#[pymethods]
impl PyTraverser {
    /// Number of cells used for the traversal process. Each one of these cells
    /// will be queried using a separate DB query.
    #[getter]
    fn num_traversal_cells(&self) -> usize {
        let trav = self.traverser.clone();
        self.runtime.block_on(async {
            let guard = trav.lock().await;
            guard.num_traversal_cells
        })
    }

    fn __len__(&self) -> usize {
        let trav = self.traverser.clone();
        self.runtime.block_on(async {
            let guard = trav.lock().await;
            guard.num_traversal_cells
        })
    }

    /// The H3 resolution used for the traversal process
    #[getter]
    fn traversal_h3_resolution(&self) -> u8 {
        let trav = self.traverser.clone();
        self.runtime.block_on(async {
            let guard = trav.lock().await;
            guard.traversal_h3_resolution
        })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(slf: PyRefMut<'_, Self>) -> PyResult<Option<PyObject>> {
        debug!("waiting to receive dataframe of traversed cell");
        loop {
            let trav = slf.traverser.clone();
            match slf.runtime.block_on(async {
                let mut guard = trav.lock().await;
                timeout(Duration::from_millis(500), guard.next()).await
            }) {
                Ok(Some(h3df_result)) => {
                    // channel had a waiting message
                    return Ok(Some(
                        h3df_result
                            .into_pyresult()?
                            .contained_data
                            .to_dataframewrapper()?,
                    ));
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
        options: PyTraversalOptions,
    ) -> PyResult<Self> {
        let inner_options = TraversalOptions {
            query,
            h3_resolution,
            max_h3indexes_fetch_count: options.max_h3indexes_fetch_count,
            num_connections: options.num_connections,
            filter_query: options.filter_query,
            do_uncompact: options.do_uncompact,
            ..Default::default()
        };

        let area: TraversalArea = if let Ok(geointerface) = GeoInterface::extract(area_of_interest)
        {
            geointerface.0.into()
        } else if area_of_interest.is_instance_of::<PyArray1<u64>>()? {
            let validated_cells: Vec<H3Cell> =
                indexes_from_numpy(area_of_interest.extract::<PyReadonlyArray1<u64>>()?)?;
            validated_cells.into()
        } else {
            return Err(PyValueError::new_err(
                "unsupported type for area_of_interest",
            ));
        };
        let traverser = conn
            .runtime
            .block_on(async {
                traverse(
                    &mut conn.client,
                    conn.database_name.clone(),
                    tableset_name,
                    &area,
                    inner_options,
                )
                .await
            })
            .into_pyresult()?;

        Ok(PyTraverser {
            traverser: Arc::new(Mutex::new(traverser)),
            runtime: conn.runtime.clone(),
        })
    }
}

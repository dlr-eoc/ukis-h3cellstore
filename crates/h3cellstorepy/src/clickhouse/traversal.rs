use h3cellstore::clickhouse::compacted_tables::{CompactedTablesStore, TableSet, TableSetQuery};
use numpy::{PyArray1, PyReadonlyArray1};
use py_geo_interface::GeoInterface;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tracing::log::{info, warn};

use crate::clickhouse::grpc::GRPCConnection;
use crate::error::IntoPyResult;
use h3cellstore::export::arrow_h3::export::h3ron::iter::change_resolution;
use h3cellstore::export::arrow_h3::export::h3ron::{H3Cell, ToH3Cells};

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
    max_fetch_count: usize,
}

impl Default for TraversalOptions {
    fn default() -> Self {
        Self {
            max_fetch_count: 8_000,
        }
    }
}

impl TraversalOptions {
    pub(crate) fn extract<'a>(dict: Option<&'a PyDict>) -> PyResult<Self> {
        let mut kwargs = Self::default();
        if let Some(dict) = dict {
            if let Some(mfc) = extract_dict_item_option(dict, "max_fetch_count")? {
                kwargs.max_fetch_count = mfc;
            }
        }
        Ok(kwargs)
    }
}

#[pyclass]
pub struct PyTraverser {
    query: TableSetQuery,
    traversal_cells: Vec<H3Cell>,
    traversal_h3_resolution: u8,
}

#[pymethods]
impl PyTraverser {
    #[getter]
    fn num_traversal_cells(&self) -> usize {
        self.traversal_cells.len()
    }

    fn __len__(&self) -> usize {
        self.traversal_cells.len()
    }

    #[getter]
    fn traversal_h3_resolution(&self) -> u8 {
        self.traversal_h3_resolution
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

        Ok(Self {
            query,
            traversal_cells,
            traversal_h3_resolution,
        })
    }
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

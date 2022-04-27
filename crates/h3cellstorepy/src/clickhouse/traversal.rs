use geo_types::Geometry;
use h3cellstore::clickhouse::compacted_tables::TableSet;
use numpy::PyReadonlyArray1;
use py_geo_interface::GeoInterface;
use pyo3::prelude::*;
use tracing::log::{info, warn};

use crate::error::IntoPyResult;
use h3cellstore::export::arrow_h3::export::h3ron::iter::change_resolution;
use h3cellstore::export::arrow_h3::export::h3ron::{H3Cell, HasH3Resolution, ToH3Cells};

use crate::utils::indexes_from_numpy;

enum Strategy {
    Geometry {
        geom: Geometry<f64>,
        h3_resolution: u8,
    },
    Cells {
        cells: Vec<H3Cell>,
        h3_resolution: u8,
    },
}

impl Strategy {
    pub fn name(&self) -> &str {
        match self {
            Strategy::Geometry { .. } => "Geometry",
            Strategy::Cells { .. } => "Cells",
        }
    }

    pub fn traversal_cells(
        &self,
        tableset: &TableSet,
        max_fetch_count: usize,
    ) -> PyResult<Vec<H3Cell>> {
        let traversal_resolution =
            select_traversal_resolution(tableset, self.h3_resolution(), max_fetch_count);
        match self {
            Strategy::Geometry { geom, .. } => Ok(geom
                .to_h3_cells(traversal_resolution)
                .into_pyresult()?
                .iter()
                .collect()),
            Strategy::Cells { cells, .. } => {
                let mut traversal_cells = change_resolution(cells.iter(), traversal_resolution)
                    .collect::<Result<Vec<_>, _>>()
                    .into_pyresult()?;
                traversal_cells.sort_unstable();
                traversal_cells.dedup();
                Ok(traversal_cells)
            }
        }
    }
}

impl HasH3Resolution for Strategy {
    fn h3_resolution(&self) -> u8 {
        match self {
            Strategy::Geometry { h3_resolution, .. } => *h3_resolution,
            Strategy::Cells { h3_resolution, .. } => *h3_resolution,
        }
    }
}

#[pyclass]
pub struct TraversalStrategy {
    strategy: Strategy,
}

#[pymethods]
impl TraversalStrategy {
    #[staticmethod]
    pub fn from_geometry(geo_interface: GeoInterface, h3_resolution: u8) -> Self {
        Self {
            strategy: Strategy::Geometry {
                geom: geo_interface.0,
                h3_resolution,
            },
        }
    }

    #[staticmethod]
    pub fn from_cells(cell_h3indexes: PyReadonlyArray1<u64>, h3_resolution: u8) -> PyResult<Self> {
        let cells = indexes_from_numpy(cell_h3indexes)?;
        Ok(Self {
            strategy: Strategy::Cells {
                cells,
                h3_resolution,
            },
        })
    }

    pub fn name(&self) -> String {
        self.strategy.name().to_string()
    }

    pub fn h3_resolution(&self) -> u8 {
        self.strategy.h3_resolution()
    }
}

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

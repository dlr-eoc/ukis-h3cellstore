use geo_types::Geometry;
use numpy::PyReadonlyArray1;
use py_geo_interface::GeoInterface;
use pyo3::prelude::*;

use h3cellstore::export::arrow_h3::export::h3ron::{H3Cell, HasH3Resolution};

use crate::utils::cells_from_numpy;

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
        let cells = cells_from_numpy(cell_h3indexes)?;
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

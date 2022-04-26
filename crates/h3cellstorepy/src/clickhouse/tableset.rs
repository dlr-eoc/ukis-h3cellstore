use h3cellstore::clickhouse::compacted_tables::TableSet;
use pyo3::prelude::*;

#[pyclass]
pub struct PyTableSet {
    tableset: TableSet,
}

#[pymethods]
impl PyTableSet {
    #[getter]
    pub fn basename(&self) -> String {
        self.tableset.basename.clone()
    }

    #[getter]
    pub fn compacted_resolutions(&self) -> Vec<u8> {
        self.tableset.compacted_resolutions()
    }

    #[getter]
    pub fn base_resolutions(&self) -> Vec<u8> {
        self.tableset.base_resolutions()
    }
}

impl From<TableSet> for PyTableSet {
    fn from(tableset: TableSet) -> Self {
        Self { tableset }
    }
}

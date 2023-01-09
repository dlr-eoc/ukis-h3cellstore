use pyo3::prelude::*;
use ukis_h3cellstore::clickhouse::compacted_tables::TableSet;

/// A Tableset describes the available database tables of a schema created from a `CompactedTableSchema`
#[pyclass]
pub struct PyTableSet {
    tableset: TableSet,
}

#[pymethods]
impl PyTableSet {
    /// The name of the TableSet.
    ///
    /// Matches `CompactedTableSchema.name`.
    #[getter]
    pub fn basename(&self) -> String {
        self.tableset.basename.clone()
    }

    /// All compacted resolutions available in the tableset
    #[getter]
    pub fn compacted_resolutions(&self) -> Vec<u8> {
        self.tableset.compacted_resolutions()
    }

    /// All base resolutions available in the tableset
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

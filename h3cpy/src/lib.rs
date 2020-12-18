mod window;
mod inspect;

use pyo3::{
    prelude::*,
    wrap_pyfunction,
    Python
};
use numpy::{PyArray, Ix1, IntoPyArray};
use crate::{
    inspect::CompactedTable
};
use h3::index::Index;

/// version of the module
#[pyfunction]
fn version() -> PyResult<String> { Ok(env!("CARGO_PKG_VERSION").to_string()) }

#[pyclass]
#[derive(Clone)]
struct ClickhouseConnection {}

#[pymethods]
impl ClickhouseConnection {

    #[new]
    pub fn new() -> Self {
        Self {}
    }

    /// proof-of-concept for numpy integration. using u64 as this will be the type for h3 indexes
    /// TODO: remove later
    pub fn poc_some_h3indexes(&self) -> PyResult<Py<PyArray<u64, Ix1>>> {
        let idx: Index = 0x89283080ddbffff_u64.into();
        let v: Vec<_> = idx.k_ring(80).iter().map(|i| i.h3index()).collect();
        let gil = Python::acquire_gil();
        let py = gil.python();
        Ok(v.into_pyarray(py).to_owned())
    }
}


/// A Python module implemented in Rust.
#[pymodule]
fn h3cpy(py: Python, m: &PyModule) -> PyResult<()> {
    m.add("CompactedTable", py.get_type::<CompactedTable>())?;
    m.add("ClickhouseConnection", py.get_type::<ClickhouseConnection>())?;
    m.add_function(wrap_pyfunction!(version, m)?)?;

    Ok(())
}
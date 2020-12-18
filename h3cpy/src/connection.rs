use pyo3::{
    prelude::*,
    PyResult,
    Py,
    Python
};
use numpy::{PyArray, IntoPyArray, Ix1};
use h3::index::Index;

#[pyclass]
#[derive(Clone)]
pub struct ClickhouseConnection {}

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


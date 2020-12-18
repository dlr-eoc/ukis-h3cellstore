mod window;
mod inspect;

use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use numpy::{PyArray, Ix1, IntoPyArray};
use crate::{
    inspect::CompactedTable
};
use h3::index::Index;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

/// version of the module
#[pyfunction]
fn version() -> PyResult<String> { Ok(env!("CARGO_PKG_VERSION").to_string()) }



/// A Python module implemented in Rust.
#[pymodule]
fn h3cpy(py: Python, m: &PyModule) -> PyResult<()> {
    m.add("CompactedTable", py.get_type::<CompactedTable>())?;
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_function(wrap_pyfunction!(version, m)?)?;

    /// proof-of-concept for numpy integration. using u64 as this will be the type for h3 indexes
    /// TODO: remove later
    #[pyfn(m, "poc_some_h3indexes")]
    fn poc_some_h3indexes(py: Python) -> &PyArray<u64, Ix1> {
        let idx: Index = 0x89283080ddbffff_u64.into();
        let v: Vec<_> = idx.k_ring(80).iter().map(|i| i.h3index()).collect();
        v.into_pyarray(py)
    }
    Ok(())
}
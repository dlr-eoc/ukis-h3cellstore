mod window;
mod inspect;
mod connection;

use pyo3::{
    prelude::*,
    wrap_pyfunction,
    Python
};
use crate::{
    inspect::CompactedTable,
    connection::ClickhouseConnection,
};

/// version of the module
#[pyfunction]
fn version() -> PyResult<String> { Ok(env!("CARGO_PKG_VERSION").to_string()) }


/// A Python module implemented in Rust.
#[pymodule]
fn h3cpy(py: Python, m: &PyModule) -> PyResult<()> {
    m.add("CompactedTable", py.get_type::<CompactedTable>())?;
    m.add("ClickhouseConnection", py.get_type::<ClickhouseConnection>())?;
    m.add_function(wrap_pyfunction!(version, m)?)?;

    Ok(())
}
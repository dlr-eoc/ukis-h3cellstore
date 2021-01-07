mod window;
mod inspect;
mod connection;
mod geometry;

use pyo3::{
    prelude::*,
    wrap_pyfunction,
    Python
};
use crate::{
    inspect::{
        CompactedTable,
        TableSet
    },
    connection::{
        ClickhouseConnection,
        ResultSet,
    },
};

/// version of the module
#[pyfunction]
fn version() -> PyResult<String> { Ok(env!("CARGO_PKG_VERSION").to_string()) }


/// A Python module implemented in Rust.
#[pymodule]
fn h3cpy(py: Python, m: &PyModule) -> PyResult<()> {
    m.add("CompactedTable", py.get_type::<CompactedTable>())?;
    m.add("TableSet", py.get_type::<TableSet>())?;
    m.add("ClickhouseConnection", py.get_type::<ClickhouseConnection>())?;
    m.add("ResultSet", py.get_type::<ResultSet>())?;
    m.add_function(wrap_pyfunction!(version, m)?)?;

    Ok(())
}
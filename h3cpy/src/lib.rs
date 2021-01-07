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
        RuntimedPool,
        ResultSet,
    },
};

/// version of the module
#[pyfunction]
fn version() -> PyResult<String> { Ok(env!("CARGO_PKG_VERSION").to_string()) }

/// open a connection to clickhouse
#[pyfunction]
fn create_connection(db_url: &str) -> PyResult<ClickhouseConnection> {
    Ok(ClickhouseConnection {
        rp: RuntimedPool::create(db_url)?
    })
}

/// A Python module implemented in Rust.
#[pymodule]
fn h3cpy(py: Python, m: &PyModule) -> PyResult<()> {

    env_logger::init();

    m.add("CompactedTable", py.get_type::<CompactedTable>())?;
    m.add("TableSet", py.get_type::<TableSet>())?;
    m.add("ClickhouseConnection", py.get_type::<ClickhouseConnection>())?;
    m.add("ResultSet", py.get_type::<ResultSet>())?;
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(create_connection, m)?)?;

    Ok(())
}
use pyo3::{
    prelude::*,
    Python,
    wrap_pyfunction,
};

use crate::{
    clickhouse::{
        RuntimedPool,
    },
    connection::{
        ClickhouseConnection,
        ResultSet,
    },
    inspect::{
        CompactedTable,
        TableSet,
    },
};

mod window;
mod inspect;
mod connection;
mod geometry;
mod clickhouse;

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
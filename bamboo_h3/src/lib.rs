use std::sync::Arc;

use numpy::PyReadonlyArray1;
use pyo3::{
    prelude::*,
    wrap_pyfunction, Python,
};

use crate::{
    clickhouse::{validate_clickhouse_url, ClickhouseConnection, ResultSet},
    convert::ColumnSet,
    inspect::{CompactedTable, TableSet},
    syncapi::ClickhousePool,
};

mod clickhouse;
mod convert;
mod geo;
mod inspect;
mod syncapi;
mod window;

/// version of the module
#[pyfunction]
fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

/// open a connection to clickhouse
#[pyfunction]
fn create_connection(db_url: &str) -> PyResult<ClickhouseConnection> {
    validate_clickhouse_url(db_url)?;
    Ok(ClickhouseConnection::new(Arc::new(ClickhousePool::create(
        db_url,
    )?)))
}


/// calculate the convex hull of an array og h3 indexes
#[pyfunction]
fn h3indexes_convex_hull(np_array: PyReadonlyArray1<u64>) -> crate::geo::Polygon {
    let view = np_array.as_array();
    bamboo_h3_int::algorithm::h3indexes_convex_hull(&view).into()
}

/// A Python module implemented in Rust.
#[pymodule]
fn bamboo_h3(py: Python, m: &PyModule) -> PyResult<()> {
    env_logger::init();

    m.add("CompactedTable", py.get_type::<CompactedTable>())?;
    m.add("TableSet", py.get_type::<TableSet>())?;
    m.add(
        "ClickhouseConnection",
        py.get_type::<ClickhouseConnection>(),
    )?;
    m.add("ResultSet", py.get_type::<ResultSet>())?;
    m.add("Polygon", py.get_type::<crate::geo::Polygon>())?;
    m.add(
        "H3IndexesContainedIn",
        py.get_type::<crate::geo::H3IndexesContainedIn>(),
    )?;
    m.add(
        "SlidingH3Window",
        py.get_type::<crate::window::SlidingH3Window>(),
    )?;
    m.add("ColumnSet", py.get_type::<ColumnSet>())?;

    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(create_connection, m)?)?;
    m.add_function(wrap_pyfunction!(h3indexes_convex_hull, m)?)?;

    Ok(())
}

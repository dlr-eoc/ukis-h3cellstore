#[macro_use]
extern crate lazy_static;

use numpy::PyReadonlyArray1;
use pyo3::{prelude::*, wrap_pyfunction, Python};

use bamboo_h3_core::clickhouse::validate_clickhouse_url;

use crate::error::IntoPyResult;
use crate::{
    clickhouse::{ClickhouseConnection, ResultSet},
    columnset::ColumnSet,
    inspect::{CompactedTable, TableSet},
    syncapi::ClickhousePool,
};
use tracing_subscriber::EnvFilter;

mod clickhouse;
mod columnset;
mod env;
mod error;
mod geo;
mod inspect;
mod schema;
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
    Ok(ClickhouseConnection::new(ClickhousePool::create(
        &validate_clickhouse_url(db_url, Some(2000)).into_pyresult()?,
    )?))
}

/// calculate the convex hull of an array og h3 indexes
#[pyfunction]
fn h3indexes_convex_hull(np_array: PyReadonlyArray1<u64>) -> PyResult<crate::geo::Polygon> {
    let view = np_array.as_array();
    let poly = bamboo_h3_core::algorithm::h3indexes_convex_hull(&view).into_pyresult()?;
    Ok(poly.into())
}

#[pyfunction]
pub fn intersect_columnset_with_indexes(
    py: Python,
    cs: &ColumnSet,
    wkbs: Vec<&[u8]>,
    h3indexes: PyReadonlyArray1<u64>,
) -> PyResult<ColumnSet> {
    crate::geo::intersect_columnset_with_indexes(py, cs, wkbs, h3indexes)
}

#[cfg(debug_assertions)]
#[pyfunction]
fn is_release_build() -> bool {
    false
}

#[cfg(not(debug_assertions))]
#[pyfunction]
fn is_release_build() -> bool {
    true
}

/// A Python module implemented in Rust.
#[pymodule]
fn bamboo_h3(py: Python, m: &PyModule) -> PyResult<()> {

    tracing_subscriber::fmt()
        //.event_format(tracing_subscriber::fmt::format::json()) // requires json feature
        //.with_max_level(tracing::Level::TRACE)
        .with_env_filter(EnvFilter::from_default_env())
        .with_timer(tracing_subscriber::fmt::time())
        .init();


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

    // **** schema *****************************************
    m.add("Schema", py.get_type::<crate::schema::Schema>())?;
    m.add(
        "CompactedTableSchemaBuilder",
        py.get_type::<crate::schema::CompactedTableSchemaBuilder>(),
    )?;

    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(create_connection, m)?)?;
    m.add_function(wrap_pyfunction!(h3indexes_convex_hull, m)?)?;
    m.add_function(wrap_pyfunction!(is_release_build, m)?)?;
    m.add_function(wrap_pyfunction!(intersect_columnset_with_indexes, m)?)?;

    Ok(())
}

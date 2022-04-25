mod clickhouse;
mod error;
mod frame;

use pyo3::{prelude::*, wrap_pyfunction, Python};
use tracing_subscriber::EnvFilter;

use crate::clickhouse::init_clickhouse_submodule;
use crate::frame::{PyDataFrame, PyH3DataFrame};

/// version of the module
#[pyfunction]
fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
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

#[pymodule]
fn h3cellstorepy(py: Python, m: &PyModule) -> PyResult<()> {
    tracing_subscriber::fmt()
        //.event_format(tracing_subscriber::fmt::format::json()) // requires json feature
        //.with_max_level(tracing::Level::TRACE)
        .with_env_filter(EnvFilter::from_default_env())
        .with_timer(tracing_subscriber::fmt::time())
        .init();

    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(is_release_build, m)?)?;

    m.add("PyH3DataFrame", py.get_type::<PyH3DataFrame>())?;
    m.add("PyDataFrame", py.get_type::<PyDataFrame>())?;

    let clickhouse_submod = PyModule::new(py, "clickhouse")?;
    init_clickhouse_submodule(py, clickhouse_submod)?;
    m.add_submodule(clickhouse_submod)?;

    Ok(())
}

use crate::clickhouse::schema::{CompactedTableSchema, CompactedTableSchemaBuilder};
use pyo3::prelude::PyModule;
use pyo3::{PyResult, Python};

pub mod schema;

pub fn init_clickhouse_submodule(py: Python, m: &PyModule) -> PyResult<()> {
    m.add(
        "CompactedTableSchema",
        py.get_type::<CompactedTableSchema>(),
    )?;
    m.add(
        "CompactedTableSchemaBuilder",
        py.get_type::<CompactedTableSchemaBuilder>(),
    )?;
    Ok(())
}

use crate::clickhouse::grpc::{GRPCConnection, GRPCRuntime, PyInsertOptions, PyTableSetQuery};
use crate::clickhouse::schema::{
    PyCompactedTableSchema, PyCompactedTableSchemaBuilder, PyCompressionMethod,
};
use crate::clickhouse::tableset::PyTableSet;
use crate::clickhouse::traversal::PyTraverser;
use pyo3::prelude::PyModule;
use pyo3::{PyResult, Python};

mod grpc;
mod schema;
mod tableset;
mod traversal;

pub fn init_clickhouse_submodule(py: Python, m: &PyModule) -> PyResult<()> {
    m.add(
        "CompactedTableSchema",
        py.get_type::<PyCompactedTableSchema>(),
    )?;
    m.add(
        "CompactedTableSchemaBuilder",
        py.get_type::<PyCompactedTableSchemaBuilder>(),
    )?;
    m.add("GRPCRuntime", py.get_type::<GRPCRuntime>())?;
    m.add("GRPCConnection", py.get_type::<GRPCConnection>())?;
    m.add("TableSet", py.get_type::<PyTableSet>())?;
    m.add("InsertOptions", py.get_type::<PyInsertOptions>())?;
    m.add("TableSetQuery", py.get_type::<PyTableSetQuery>())?;
    m.add("CompressionMethod", py.get_type::<PyCompressionMethod>())?;
    m.add("Traverser", py.get_type::<PyTraverser>())?;
    Ok(())
}

use crate::clickhouse::grpc::{GRPCConnection, GRPCRuntime};
use crate::clickhouse::schema::{CompactedTableSchema, CompactedTableSchemaBuilder};
use crate::clickhouse::traversal::TraversalStrategy;
use pyo3::prelude::PyModule;
use pyo3::{PyResult, Python};

mod grpc;
mod schema;
mod traversal;

pub fn init_clickhouse_submodule(py: Python, m: &PyModule) -> PyResult<()> {
    m.add(
        "CompactedTableSchema",
        py.get_type::<CompactedTableSchema>(),
    )?;
    m.add(
        "CompactedTableSchemaBuilder",
        py.get_type::<CompactedTableSchemaBuilder>(),
    )?;
    m.add("TraversalStrategy", py.get_type::<TraversalStrategy>())?;
    m.add("GRPCRuntime", py.get_type::<GRPCRuntime>())?;
    m.add("GRPCConnection", py.get_type::<GRPCConnection>())?;
    Ok(())
}

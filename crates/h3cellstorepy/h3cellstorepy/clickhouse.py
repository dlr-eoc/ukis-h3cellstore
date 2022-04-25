
from .h3cellstorepy import clickhouse

CompactedTableSchema = clickhouse.CompactedTableSchema
CompactedTableSchemaBuilder = clickhouse.CompactedTableSchemaBuilder
TraversalStrategy = clickhouse.TraversalStrategy

# default grpc/tokio runtime with 3 threads
_RUNTIME = clickhouse.GRPCRuntime(3)

__all__ = [
    # accessing the imported function and classes to let IDEs know these are not
    # unused imports. They are only re-exported, but not used in this file.
    CompactedTableSchema.__name__,
    CompactedTableSchemaBuilder.__name__,
    TraversalStrategy.__name__,
]


def connect_grpc(grpc_endpoint: str, database_name: str, create_db: bool = False) -> clickhouse.GRPCConnection:
    return clickhouse.GRPCConnection.connect(grpc_endpoint, database_name, create_db, _RUNTIME)

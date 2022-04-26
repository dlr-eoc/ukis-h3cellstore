# noinspection PyUnresolvedReferences
from .h3cellstorepy import clickhouse

CompactedTableSchema = clickhouse.CompactedTableSchema
CompactedTableSchemaBuilder = clickhouse.CompactedTableSchemaBuilder
TraversalStrategy = clickhouse.TraversalStrategy
GRPCConnection = clickhouse.GRPCConnection

# default grpc/tokio runtime with 3 threads
_RUNTIME = clickhouse.GRPCRuntime(3)


def connect(grpc_endpoint: str, database_name: str, create_db: bool = False) -> GRPCConnection:
    return clickhouse.GRPCConnection.connect(grpc_endpoint, database_name, create_db, _RUNTIME)

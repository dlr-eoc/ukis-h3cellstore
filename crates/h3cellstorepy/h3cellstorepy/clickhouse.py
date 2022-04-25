# noinspection PyUnresolvedReferences
from .h3cellstorepy import clickhouse
from .frame import DataFrameWrapper

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


class GRPCConnection:
    _connection: clickhouse.GRPCConnection

    def __init__(self, grpc_endpoint: str, database_name: str, create_db: bool = False):
        self._connection = clickhouse.GRPCConnection.connect(grpc_endpoint, database_name, create_db, _RUNTIME)

    def execute(self, query):
        """execute the given query in the database without returning any result"""
        return self._connection.execute(query)

    def execute_into_dataframe(self, query: str) -> DataFrameWrapper:
        """execute the given query and return a non-H3 dataframe of it"""
        return DataFrameWrapper(self._connection.execute_into_dataframe(query))

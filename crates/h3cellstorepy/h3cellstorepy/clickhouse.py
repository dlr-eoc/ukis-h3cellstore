# noinspection PyUnresolvedReferences
from .h3cellstorepy import clickhouse

__all__ = []

# bring everything into scope
for cls_name in clickhouse.__all__:
    locals()[cls_name] = getattr(clickhouse, cls_name)
    __all__.append(cls_name)

# default grpc/tokio runtime with 3 threads
_RUNTIME = clickhouse.GRPCRuntime(3)

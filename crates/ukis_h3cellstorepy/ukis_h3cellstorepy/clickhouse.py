# noinspection PyUnresolvedReferences
from .ukis_h3cellstorepy import clickhouse

__all__ = []

# bring everything into scope
for cls_name in clickhouse.__all__:
    locals()[cls_name] = getattr(clickhouse, cls_name)
    __all__.append(cls_name)

# default grpc/tokio runtime with a number of worker threads matching the cores
# of the system
_RUNTIME = clickhouse.GRPCRuntime()

# h3cellstore

Python bindings to integrate clickhouse H3 databases with the python data-science world.

## Contents

- **arrow_h3**: H3-related algorithms on dataframes containing H3 cells
- **clickhouse_arrow_grpc**: GRPC-based interface library for ClickHouse using Arrow-IPC as data exchange format
- **h3cellstore**: High-level rust crate to store H3 cells in ClickHouse databases
- **h3cellstorepy**: High-level Python library to store H3 cells in ClickHouse databases

See `crates` subdirectory.

## Inner workings

### `Compacted tables` storage schema

![](doc/img/storing-dataframes.svg)

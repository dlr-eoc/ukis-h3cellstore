# clickhouse_arrow_grpc

GPRC-based interface to Clickhouse to directly exchange arrow-based data

* Uses [polars](https://www.pola.rs/) as dataframe abstraction over arrow data.
* Improves on [Clickhouse default type mappings](https://clickhouse.com/docs/en/interfaces/formats/#data_types-matching-arrow) 
  * auto-converting strings and booleans based on Clickhouse column types

## Building

Requires `protoc` - the protobuf compiler - to b available on the system ([tonic #1047](https://github.com/hyperium/tonic/issues/1047)). Install on ubuntu:

```shell
apt install -y protobuf-compiler libprotobuf-dev
```

## Run the examples

Launch a ClickHouse server as described in the main README. After that, you should be able to run the example:

```shell
cargo run --example helloworld
```

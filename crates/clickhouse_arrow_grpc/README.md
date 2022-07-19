# clickhouse_arrow_grpc

GPRC-based interface to Clickhouse to directly exchange arrow-based data

* Uses [polars](https://www.pola.rs/) as dataframe abstraction over arrow data.
* Improves on [Clickhouse default type mappings](https://clickhouse.com/docs/en/interfaces/formats/#data_types-matching-arrow) 
  * auto-converting strings and booleans based on Clickhouse column types

## Run the examples

Launch a ClickHouse server as described in the main README. After that, you should be able to run the example:

```shell
cargo run --example helloworld
```

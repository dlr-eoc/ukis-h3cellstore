# GPRC based interface to Clickhouse to directly exchange arrow-based data

* Uses [polars](https://www.pola.rs/) as dataframe abstraction over arrow data.
* Improves on [Clickhouse default type mappings](https://clickhouse.com/docs/en/interfaces/formats/#data_types-matching-arrow) by auto-converting strings based on Clickhouse column types.

## Launching a Clickhouse instance for the example to work

```shell
podman run --rm -it -u 101 -v $PWD/clickhouse-server/config.xml:/etc/clickhouse-server/config.xml -p 9100:9100 clickhouse/clickhouse-server:22.3
```

After that, you should be able to run the example:

```shell
cargo run --example helloworld
```

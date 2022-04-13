

# Launching Clickhouse for the example to work

```shell
podman run --rm -it -u 101 -v $PWD/clickhouse-server/config.xml:/etc/clickhouse-server/config.xml -p 9100:9100 clickhouse/clickhouse-server:22.3
```

After that, you should be able to run the example:

```shell
cargo run --example helloworld
```

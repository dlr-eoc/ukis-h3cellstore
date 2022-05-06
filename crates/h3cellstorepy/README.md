
## Run the python unittests

To include the database tests, launch a ClickHouse server as described in the main README.

After that, you should be able to run the tests using:

```shell
export CLICKHOUSE_GRPC_TESTING_ENDPOINT="http://127.0.0.1:9100"
export RUST_LOG=debug
export PYTHONUNBUFFERED=1
pytest -s
```


## Run the rust unittests

```shell
cargo test --no-default-features
```

## Production build

```shell
just build-prod
ls -lah ../../target/wheels/
```

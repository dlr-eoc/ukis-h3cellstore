
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
# see https://github.com/pola-rs/polars/blob/9a3066943eda6a0b96807b4d6f6271645a4c55cc/.github/deploy_manylinux.sh#L12
export RUSTFLAGS='-C target-feature=+fxsr,+sse,+sse2,+sse3,+ssse3,+sse4.1,+sse4.2,+popcnt,+avx,+fma'
maturin build --release --strip
ls -lah ../../target/wheels/
```

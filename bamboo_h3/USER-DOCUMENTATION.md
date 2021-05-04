# User documentation

## Connecting to clickhouse

This library uses [clickhouse_rs](https://github.com/suharev7/clickhouse-rs), so all the connection options
from [the documentation there](https://docs.rs/clickhouse-rs/1.0.0-alpha.1/clickhouse_rs/index.html#dns)
can be used. A few things to keep in mind:

* Always use the cheap `lz4` compression. This reduces the amount of data to be transferred over the network.
* The default `connection_timeout` in `clickhouse_rs` is with `500ms` quite low for large amounts of geodata. bamboo
  increases that to `2000ms` when nothing else is specified. Depending on what you are doing, you may need to increase
  that.

## Things to keep in mind

### ... when working with sliding windows

* Always make sure that the ranges (e.g. time range of a query) stay **static** among all windows and is not dependent
  on data found in window. Not doing so can lead to confusing differences in your results depending on the **range of
  data found in that window** - things will get even more confusing when the geographical size of the window changes (
  for example when using a different value for `MAX_WORKERS`, which will cut the AOI into different sized chunks).


## Configuration

While this library are controlled via python code, there are e few environment variables for different configuration
aspects.

The relevant implementations can be found in [env.rs](src/env.rs).

### Logging

This library uses [tracing.rs](https://tracing.rs/tracing/) (compatible to
rusts [log crate](https://docs.rs/log/0.4.6/log/) together with
the [env_logger crate](https://docs.rs/env_logger/0.8.2/env_logger/)). This means that logging to `stdout` can be
controlled via environment variables. Set the `RUST_LOG` variable to `debug`, `error`, `info`, `warn`, or `trace` for
the corresponding log output. This will give you log messages from all libraries used, most of them will be
from `clickhouse_rs`. To just get the messages from `bamboo_h3` use:

```
RUST_LOG=bamboo_h3=debug python my-script.py
```

For more fine-grained logging settings, see the documentation of `tracing` or `env_logger`.

### Window iteration

| env variable name | description | default value |
| --- | --- | --- |
| `BAMBOO_WINDOW_NUM_CLICKHOUSE_THREADS` | Number of ClickHouse threads to use during window iteration. The more threads are used, the higher the load and memory requirements in the db server will be. As this is used for mostly non-timecritical preloading, the number can be quite low. | 2 |
| `BAMBOO_WINDOW_NUM_CONCURRENT_PRELOAD_QUERIES` | Number of concurrent queries to use to preload data for the next windows from ClickHouse. | 3 |

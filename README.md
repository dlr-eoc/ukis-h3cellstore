# h3cpy

Python bindings to integrate clickhouse H3 databases with the python data-science world.


# Usage

## Logging

This library uses rusts [log crate](https://docs.rs/log/0.4.6/log/) together with 
the [env_logger crate](https://docs.rs/env_logger/0.8.2/env_logger/). This means that logging to `stdout` can be
controlled via environment variables. Set `RUST_LOG` to `debug`, `error`, `info`, `warn`, or `trace` for the corresponding 
log output. 

For more fine-grained logging settings, see the documentation of `env_logger`.
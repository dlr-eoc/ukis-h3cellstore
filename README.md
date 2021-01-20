# h3cpy

Python bindings to integrate clickhouse H3 databases with the python data-science world.

## Goals

1. Provide an integration with the widely known Python libraries.
2. Abstraction of most storage details of H3 data.
3. Enable and encourage parallelization.
4. Handling of compute-intensive tasks on the client instead of the DB servers as the 
   clients are far easier to scale.
5. Handle compute-intensive tasks in native code instead of Python.

# Usage

## Logging

This library uses rusts [log crate](https://docs.rs/log/0.4.6/log/) together with 
the [env_logger crate](https://docs.rs/env_logger/0.8.2/env_logger/). This means that logging to `stdout` can be
controlled via environment variables. Set `RUST_LOG` to `debug`, `error`, `info`, `warn`, or `trace` for the corresponding 
log output. 

For more fine-grained logging settings, see the documentation of `env_logger`.

# other relevant libraries

* [offical h3 bindings](https://github.com/uber/h3-py)
* [h3ronpy](https://github.com/nmandery/h3ron/tree/master/h3ronpy)
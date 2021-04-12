# the python extention

## Build using [maturin](https://github.com/PyO3/maturin):

There are three main commands:

* `maturin publish` builds the crate into python packages and publishes them to pypi.
* `maturin build` builds the wheels and stores them in a folder (`target/wheels` by default), but doesn't upload them. It's possible to upload those with [twine](https://github.com/pypa/twine).
* `maturin develop` builds the crate and installs it as a python module directly in the current virtualenv.

For just using with python, build with

```
maturin build --release
```

to get an optimized build.

Run the unittests with:

```shell
maturin develop
pytest -v
```

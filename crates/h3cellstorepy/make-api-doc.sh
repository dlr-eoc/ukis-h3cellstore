#!/bin/bash

# requires all dev-dependencies to be installed. see pyproject.toml

BUILD_DIR=/tmp/h3ch-doc-build

maturin develop

# https://stackoverflow.com/questions/36237477/python-docstrings-to-github-readme-md
sphinx-apidoc -o $BUILD_DIR h3cellstorepy sphinx-apidoc --full
pushd $BUILD_DIR
make markdown
popd
cp $BUILD_DIR/_build/markdown/h3cellstorepy.md API.md

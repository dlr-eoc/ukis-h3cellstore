#!/bin/bash
# build the python extension using the gzs python docker image, as the extension
# needs to be build against the same python major+minor version as it is intended to
# be used with.

set -eux

SCRIPT_DIR=$(realpath "$(dirname $0)")

cat <<EOF | sudo docker run -i --rm -v $SCRIPT_DIR:/build eoc-gzs-db01-vm.eoc.dlr.de:4001/gzs-python-base bash
set -eux

# dependencies
apt-get update
apt-get install --no-install-recommends -y curl cmake clang build-essential git
curl --proto '=https' --tlsv1.2 -sSf -o rustup.sh https://sh.rustup.rs
chmod +x rustup.sh
./rustup.sh -y
source ~/.cargo/env

cd /build/h3cpy

python3 -m pip install -r requirements.dev.txt
maturin build --release

EOF

sudo chown -R "$USER" target

# the packages can now be uploaded with
# twine upload --repository-url https://eoc-gzs-db01-vm.eoc.dlr.de:8080/repository/py-internal/ target/wheels/*`uname -p`.whl


# image to build bamboo and create python wheels

FROM rust:1.60-bullseye

RUN apt-get update && \
    apt-get install git python3-dev python3-pip --no-install-recommends build-essential clang make curl python3-setuptools python3-venv zip --yes && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/ && \
    rustup component add clippy && \
    rustup component add rustfmt

# we need cmake newer than what bullseye provides, so here we go
RUN cd /tmp && \
    curl -L https://github.com/Kitware/CMake/releases/download/v3.23.1/cmake-3.23.1.tar.gz -o cmake.tar.gz && \
    tar xf cmake.tar.gz && \
    cd cmake-3.23.1 && \
    ./bootstrap && \
    make -j4 && \
    make install && \
    cd .. && \
    rm -rf cmake-3.23.1

RUN cargo install just && rm -rf ~/.cargo && cp `which just` /usr/local/bin

# then tag as
# eoc-gzs-db01-vm.eoc.dlr.de:4002/h3cellstore-builder:latest

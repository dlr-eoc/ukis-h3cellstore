# image to build bamboo and create python wheels
#
# builds manylinux_2_24 compatible wheels

FROM rust:1.60-buster

RUN apt-get update && \
    apt-get install git python3-dev python3-pip --no-install-recommends clang cmake make curl python3-setuptools python3-venv zip --yes && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/

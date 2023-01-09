# clickhouse_arrow_grpc

GPRC-based interface to Clickhouse to directly exchange arrow-based data

* Uses [polars](https://www.pola.rs/) as dataframe abstraction over arrow data.
* Improves on [Clickhouse default type mappings](https://clickhouse.com/docs/en/interfaces/formats/#data_types-matching-arrow) 
  * auto-converting strings and booleans based on Clickhouse column types

## Building

Requires `protoc` - the protobuf compiler - to b available on the system ([tonic #1047](https://github.com/hyperium/tonic/issues/1047)). Install on ubuntu:

```shell
apt install -y protobuf-compiler libprotobuf-dev
```

## Run the examples

Launch a ClickHouse server as described in the main README. After that, you should be able to run the example:

```shell
cargo run --example helloworld
```


## Licenses
This software is licensed under the [Apache 2.0 License](https://github.com/dlr-eoc/ukis-h3cellstore/blob/master/LICENSE.txt).

Copyright (c) 2022 German Aerospace Center (DLR) * German Remote Sensing Data Center * Department: Geo-Risks and Civil Security


## Changelog
See [changelog](https://github.com/dlr-eoc/ukis-h3cellstore/blob/master/CHANGES.md).

## Contributing
The UKIS team welcomes contributions from the community.
For more detailed information, see our guide on [contributing](https://github.com/dlr-eoc/ukis-h3cellstore/blob/master/CONTRIBUTING.md) if you're interested in getting involved.

## What is UKIS?
The DLR project Environmental and Crisis Information System (the German abbreviation is UKIS, standing for [Umwelt- und Kriseninformationssysteme](https://www.dlr.de/eoc/en/desktopdefault.aspx/tabid-5413/10560_read-21914/) aims at harmonizing the development of information systems at the German Remote Sensing Data Center (DFD) and setting up a framework of modularized and generalized software components.

UKIS is intended to ease and standardize the process of setting up specific information systems and thus bridging the gap from EO product generation and information fusion to the delivery of products and information to end users.

Furthermore, the intention is to save and broaden know-how that was and is invested and earned in the development of information systems and components in several ongoing and future DFD projects.

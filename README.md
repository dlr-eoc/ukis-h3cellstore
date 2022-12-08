![logo](doc/img/logo.jpg)

# h3cellstore

Python bindings to integrate clickhouse H3 databases with the python data-science world.

Schematic of the read-workflow:

![](doc/img/h3cellstorepy-read.svg)

## Contents

- [clickhouse_arrow_grpc](crates/clickhouse_arrow_grpc/README.md): GRPC-based interface library for ClickHouse using Arrow-IPC as data exchange format
- [h3cellstore](crates/h3cellstore/README.md): High-level rust crate to store H3 cells in ClickHouse databases
- [h3cellstorepy](crates/h3cellstorepy/README.md): High-level Python library to store H3 cells in ClickHouse databases

See `crates` subdirectory.

## Inner workings

### `Compacted tables` storage schema

![](doc/img/storing-dataframes.svg)


## Development

### Launching a Clickhouse instance for the examples to work

see the `clickhouse` target in the [justfile](justfile).


<sup>
The logo has been created with <a href="https://www.craiyon.com/">Craiyon</a> and the term "illustration of happy unicorn with rainbow-tail standing on a shipping container framed within hexagon".
</sup>


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

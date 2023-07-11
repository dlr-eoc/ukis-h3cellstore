# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


**This changelog only tracke the `ukis_h3cellstorepy` python extension.**


## Unreleased

* Nullable columns and "SetNull" aggregation method on columns

## 0.12.0

* Rename all subprojects by prefixing the names with "ukis_"

## 0.11.0

## 0.10.3

- switch to h3ron-polars

## 0.10.2

- Fix of compaction bug

## 0.9.0

- Complete rewrite of all crates on basis of arrow, polars and GRPC. So there are many API changes - too 
  many to list here for the audience of this Changelog.

## 0.8.0 - never released

### Added

- Workflow template to build the extension with [argo](https://github.com/argoproj/argo-workflows/). #15
- Multi-year table partitioning. #39
- Allow passing `walk` the resolution to use for the batch size (`r_walk`). #42

### Changed

- North-south iteration order when iterating through the cells of a tableset. #38
- Switch from using the `log` crate, to the `tracing` crate. #36
- Rename `sliding_window` to `walk` as this is not really a sliding window. Choose
  the name `walk` to align with pythons `os.walk`. #37
- Cleaner module structure:  
  - move ClickHouse-related things to `bamboo_h3.clickhouse` subpackage.
  - move `Polygon`, `H3IndexesContainedIn` and `h3indexes_convex_hull` to `bamboo_h3.geo` subpackage.

### Removed

## 0.8.0

No changelog was kept in 0.8.0 and before.

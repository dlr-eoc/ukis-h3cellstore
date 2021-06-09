# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- Workflow template to build the extension with [argo](https://github.com/argoproj/argo-workflows/). #15
- Multi-year table partitioning. #39

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

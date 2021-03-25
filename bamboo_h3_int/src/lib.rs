#[macro_use]
extern crate lazy_static;

pub use clickhouse_rs;
pub use geo;
// re-export some crates for downstream bamboo_h3 (and other crates) to ensure matching
// versions
pub use geo_types;

pub use crate::colvec::ColumnSet;
pub use crate::colvec::ColVec;

pub mod algorithm;
pub mod clickhouse;
mod colvec;
pub mod error;
pub mod fileio;

/// the column name which must be used for h3indexes.
pub const COL_NAME_H3INDEX: &str = "h3index";


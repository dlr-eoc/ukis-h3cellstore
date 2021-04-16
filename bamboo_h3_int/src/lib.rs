#[macro_use]
extern crate lazy_static;

pub use clickhouse_rs;
pub use geo;
// re-export some crates for downstream bamboo_h3 (and other crates) to ensure matching
// versions
pub use geo_types;

pub use crate::columnset::ColVec;
pub use crate::columnset::ColumnSet;
pub use crate::columnset::Datatype;

pub mod algorithm;
pub mod clickhouse;
mod columnset;
mod common;
pub mod error;
pub mod fileio;
mod iter;

/// the column name which must be used for h3indexes.
pub const COL_NAME_H3INDEX: &str = "h3index";

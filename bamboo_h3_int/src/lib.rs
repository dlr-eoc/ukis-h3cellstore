#[macro_use]
extern crate lazy_static;

pub use crate::colvec::ColVec;
pub use crate::colvec::ColumnSet;

pub mod algorithm;
pub mod clickhouse;
mod colvec;
pub mod compacted_tables;
pub mod error;
pub mod window;
pub mod fileio;
pub mod schema;

// re-export some crates for downstream bamboo_h3 (and other crates) to ensure matching
// versions
pub use geo_types;
pub use geo;
pub use clickhouse_rs;


/// the column name which must be used for h3indexes.
pub const COL_NAME_H3INDEX: &str = "h3index";


//!
//!
//! Limitations:
//! * Only intended to work with H3 Cells currently. Edges, Vertices, ... are not supported.

extern crate core;

pub mod algo;
pub mod error;
pub mod frame;

pub use error::Error;
pub use frame::H3DataFrame;

#[cfg(feature = "reexport-deps")]
pub use h3ron; // for downstream dependency management
#[cfg(feature = "reexport-deps")]
pub use polars; // for downstream dependency management
#[cfg(feature = "reexport-deps")]
pub use polars_core; // for downstream dependency management

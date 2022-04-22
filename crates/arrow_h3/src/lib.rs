//!
//!
//! Limitations:
//! * Only intended to work with H3 Cells currently. Edges, Vertices, ... are not supported.

extern crate core;

#[cfg(feature = "export")]
pub mod export;

pub use error::Error;
pub use frame::H3DataFrame;

pub mod algo;
pub mod error;
pub mod frame;
pub mod series;

// for downstream dependency management

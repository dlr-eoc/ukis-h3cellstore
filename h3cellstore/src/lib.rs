#[cfg(feature = "reexport-deps")]
pub use arrow_h3;

pub use crate::error::Error;

pub mod clickhouse;
pub mod error;

pub trait Named {
    fn name(&self) -> &'static str;
}

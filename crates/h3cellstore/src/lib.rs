pub use crate::error::Error;

pub mod clickhouse;
pub mod error;
#[cfg(feature = "export")]
pub mod export;

pub trait Named {
    fn name(&self) -> &'static str;
}

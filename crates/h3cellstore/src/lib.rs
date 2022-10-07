pub use crate::error::Error;

pub mod clickhouse;
pub mod error;
pub mod export;
pub mod frame;
pub mod geom;

pub trait Named {
    fn name(&self) -> &'static str;
}

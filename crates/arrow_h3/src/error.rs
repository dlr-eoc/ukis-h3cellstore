use thiserror::Error as ThisError;

#[derive(ThisError, Debug)]
pub enum Error {
    #[error("h3ron error: {0}")]
    H3ron(#[from] h3ron::Error),

    #[error("polars error: {0}")]
    Polars(#[from] polars_core::error::PolarsError),

    #[error("dataframe h3index column '{0}' is typed as {1}, but should be UInt64")]
    DataframeInvalidH3IndexType(String, String),

    #[error("dataframe contains no column named '{0}'")]
    DataframeMissingColumn(String),

    #[error("Unsupported H3 resolution: {0}")]
    UnsupportedH3Resolution(u8),

    #[error("missing index value")]
    MissingIndexValue,
}

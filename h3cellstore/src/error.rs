use thiserror::Error as ThisError;

use arrow_h3::Error as AH3Error;
use clickhouse_arrow_grpc::Error as CAGError;

#[derive(ThisError, Debug)]
pub enum Error {
    #[error("polars error: {0}")]
    Polars(#[from] arrow_h3::polars_core::error::PolarsError),

    #[error("arrow error: {0}")]
    Arrow(#[from] clickhouse_arrow_grpc::arrow2::error::ArrowError),

    #[error("tonic GRPC status error: {0}")]
    TonicStatus(#[from] clickhouse_arrow_grpc::tonic::Status),

    #[error("ClickhouseException({name:?}, {display_text:?})")]
    ClickhouseException {
        name: String,
        display_text: String,
        stack_trace: String,
    },

    #[error("mismatch of arrays in chunk to number of casts")]
    CastArrayLengthMismatch,

    #[error("arrow chunk is missing field '{0}'")]
    ArrowChunkMissingField(String),

    #[error("join error")]
    JoinError(#[from] clickhouse_arrow_grpc::tokio::task::JoinError),

    #[error("h3ron error: {0}")]
    H3ron(#[from] arrow_h3::h3ron::Error),

    #[error("dataframe h3index column '{0}' is typed as {1}, but should be UInt64")]
    DataframeInvalidH3IndexType(String, String),

    #[error("dataframe contains no column named '{0}'")]
    DataframeMissingColumn(String),

    #[error("Unsupported H3 resolution: {0}")]
    UnsupportedH3Resolution(u8),
}

impl From<AH3Error> for Error {
    fn from(ah_error: AH3Error) -> Self {
        match ah_error {
            AH3Error::H3ron(e) => Self::H3ron(e),
            AH3Error::Polars(e) => Self::Polars(e),
            AH3Error::DataframeInvalidH3IndexType(a, b) => Self::DataframeInvalidH3IndexType(a, b),
            AH3Error::DataframeMissingColumn(column_name) => {
                Self::DataframeMissingColumn(column_name)
            }
            AH3Error::UnsupportedH3Resolution(resolution) => {
                Error::UnsupportedH3Resolution(resolution)
            }
        }
    }
}

impl From<CAGError> for Error {
    fn from(cagerror: CAGError) -> Self {
        match cagerror {
            CAGError::Polars(e) => Self::Polars(e),
            CAGError::Arrow(e) => Self::Arrow(e),
            CAGError::TonicStatus(e) => Self::TonicStatus(e),
            CAGError::ClickhouseException {
                name,
                display_text,
                stack_trace,
            } => Self::ClickhouseException {
                name,
                display_text,
                stack_trace,
            },
            CAGError::CastArrayLengthMismatch => Self::CastArrayLengthMismatch,
            CAGError::ArrowChunkMissingField(name) => Self::ArrowChunkMissingField(name),
            CAGError::JoinError(e) => Self::JoinError(e),
        }
    }
}

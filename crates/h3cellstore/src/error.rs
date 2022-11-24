use thiserror::Error as ThisError;

use clickhouse_arrow_grpc::{ClickhouseException, Error as CAGError};

#[derive(ThisError, Debug)]
pub enum Error {
    #[error(transparent)]
    Polars(#[from] polars::error::PolarsError),

    #[error(transparent)]
    H3ronPolars(#[from] h3ron_polars::error::Error),

    #[error(transparent)]
    Arrow(#[from] polars::error::ArrowError),

    #[error(transparent)]
    TonicStatus(#[from] clickhouse_arrow_grpc::export::tonic::Status),

    #[error(transparent)]
    TonicTansport(#[from] clickhouse_arrow_grpc::export::tonic::transport::Error),

    #[error("ClickhouseException({})", .0.to_string())]
    ClickhouseException(ClickhouseException),

    #[error("mismatch of arrays in chunk to number of casts")]
    CastArrayLengthMismatch,

    #[error("arrow chunk is missing field '{0}'")]
    ArrowChunkMissingField(String),

    #[error(transparent)]
    JoinError(#[from] tokio::task::JoinError),

    #[error(transparent)]
    H3ron(#[from] h3ron::Error),

    #[error("dataframe h3index column '{0}' is typed as {1}, but should be UInt64")]
    DataframeInvalidH3IndexType(String, String),

    #[error("dataframe contains no column named '{0}'")]
    DataframeMissingColumn(String),

    #[error("Unsupported H3 resolution: {0}")]
    UnsupportedH3Resolution(u8),

    #[error("no queryable tables found")]
    NoQueryableTables,

    #[error("mixed h3 resolutions")]
    MixedH3Resolutions,

    #[error("empty cells")]
    EmptyCells,

    #[error("missing query placeholder {0}")]
    MissingQueryPlaceholder(String),

    #[error("schema error validating {0}: {1}")]
    SchemaValidationError(&'static str, String),

    #[error("no h3 resolutions defined")]
    NoH3ResolutionsDefined,

    #[error("missing preconditions for partial optimization")]
    MissingPrecondidtionsForPartialOptimization,

    #[error("tableset not found: {0}")]
    TableSetNotFound(String),

    #[error("database not found: {0}")]
    DatabaseNotFound(String),

    #[error("missing index value")]
    MissingIndexValue,

    #[error("abort has been triggered")]
    Abort,

    #[error("acquiring lock failed")]
    AcquiringLockFailed,

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl From<CAGError> for Error {
    fn from(cagerror: CAGError) -> Self {
        match cagerror {
            CAGError::Polars(e) => Self::Polars(e),
            CAGError::Arrow(e) => Self::Arrow(e),
            CAGError::TonicStatus(e) => Self::TonicStatus(e),
            CAGError::ClickhouseException(ce) => Self::ClickhouseException(ce),
            CAGError::CastArrayLengthMismatch => Self::CastArrayLengthMismatch,
            CAGError::ArrowChunkMissingField(name) => Self::ArrowChunkMissingField(name),
            CAGError::JoinError(e) => Self::JoinError(e),
        }
    }
}

use crate::ClickhouseException;
use thiserror::Error as ThisError;

#[derive(ThisError, Debug)]
pub enum Error {
    #[error(transparent)]
    Polars(#[from] polars_core::error::PolarsError),

    #[error(transparent)]
    Arrow(#[from] arrow2::error::Error),

    #[error(transparent)]
    TonicStatus(#[from] tonic::Status),

    #[error("ClickhouseException({})", .0.to_string())]
    ClickhouseException(ClickhouseException),

    #[error("mismatch of arrays in chunk to number of casts")]
    CastArrayLengthMismatch,

    #[error("arrow chunk is missing field '{0}'")]
    ArrowChunkMissingField(String),

    #[error(transparent)]
    JoinError(#[from] tokio::task::JoinError),
}

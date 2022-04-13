use thiserror::Error as ThisError;

#[derive(ThisError, Debug)]
pub enum Error {
    #[error("polars error: {0}")]
    Polars(#[from] polars_core::error::PolarsError),

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow2::error::ArrowError),

    #[error("tonic GRPC status error: {0}")]
    TonicStatus(#[from] tonic::Status),

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
    JoinError(#[from] tokio::task::JoinError),
}

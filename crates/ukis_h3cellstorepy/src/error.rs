use pyo3::exceptions::{PyIOError, PyKeyboardInterrupt, PyRuntimeError, PyValueError};
use pyo3::{PyErr, PyResult};
use tracing::debug;
use ukis_h3cellstore::export::h3ron;
use ukis_h3cellstore::export::h3ron_polars::Error;
use ukis_h3cellstore::export::ukis_clickhouse_arrow_grpc::export::tonic;
use ukis_h3cellstore::export::ukis_clickhouse_arrow_grpc::ClickhouseException;

pub trait ToCustomPyErr {
    fn to_custom_pyerr(self) -> PyErr;
}

impl ToCustomPyErr for tonic::Status {
    fn to_custom_pyerr(self) -> PyErr {
        PyIOError::new_err(format!("GRPC status {}", self))
    }
}

impl ToCustomPyErr for ukis_h3cellstore::export::polars::error::PolarsError {
    fn to_custom_pyerr(self) -> PyErr {
        PyRuntimeError::new_err(format!("polars is unhappy: {:?}", self))
    }
}

impl ToCustomPyErr for ukis_h3cellstore::export::polars::error::ArrowError {
    fn to_custom_pyerr(self) -> PyErr {
        PyRuntimeError::new_err(format!("arrow error: {:?}", self))
    }
}

impl ToCustomPyErr for std::io::Error {
    fn to_custom_pyerr(self) -> PyErr {
        PyIOError::new_err(format!("{}", self))
    }
}

impl ToCustomPyErr for h3ron::Error {
    fn to_custom_pyerr(self) -> PyErr {
        PyRuntimeError::new_err(format!("h3ron error: {:?}", self))
    }
}

impl ToCustomPyErr for tokio::task::JoinError {
    fn to_custom_pyerr(self) -> PyErr {
        PyRuntimeError::new_err(format!("joining tokio task when wrong: {:?}", self))
    }
}

impl ToCustomPyErr for serde_json::Error {
    fn to_custom_pyerr(self) -> PyErr {
        PyIOError::new_err(format!("JSON (de-)serialization failed: {}", self))
    }
}

impl ToCustomPyErr
    for ukis_h3cellstore::export::ukis_clickhouse_arrow_grpc::export::tonic::transport::Error
{
    fn to_custom_pyerr(self) -> PyErr {
        PyIOError::new_err(format!("Tonic transport error: {}", self))
    }
}

impl ToCustomPyErr for ClickhouseException {
    fn to_custom_pyerr(self) -> PyErr {
        debug!(
            "clickhouse error: {} {}: stacktrace {}",
            self.name, self.display_text, self.stack_trace
        );
        PyIOError::new_err(format!(
            "Clickhouse error {}: {}",
            self.name, self.display_text
        ))
    }
}

impl ToCustomPyErr for ukis_h3cellstore::export::h3ron_polars::Error {
    fn to_custom_pyerr(self) -> PyErr {
        match self {
            Error::Polars(e) => e.to_custom_pyerr(),
            Error::Arrow(e) => e.to_custom_pyerr(),
            Error::H3ron(e) => e.to_custom_pyerr(),
            Error::SpatialIndex(_) => PyRuntimeError::new_err(self.to_string()),
            Error::InvalidH3Indexes => PyValueError::new_err(self.to_string()),
        }
    }
}

impl ToCustomPyErr for ukis_h3cellstore::Error {
    fn to_custom_pyerr(self) -> PyErr {
        match self {
            Self::Polars(e) => e.to_custom_pyerr(),
            Self::H3ron(e) => e.to_custom_pyerr(),
            Self::JoinError(e) => e.to_custom_pyerr(),
            Self::Arrow(e) => e.to_custom_pyerr(),
            Self::TonicStatus(status) => status.to_custom_pyerr(),
            Self::H3ronPolars(e) => e.to_custom_pyerr(),

            Self::MissingPrecondidtionsForPartialOptimization
            | Self::TableSetNotFound(_)
            | Self::DatabaseNotFound(_)
            | Self::Io(_)
            | Self::TonicTansport(_)
            | Self::NoQueryableTables => PyIOError::new_err(self.to_string()),

            Self::ClickhouseException(ce) => ce.to_custom_pyerr(),

            Self::AcquiringLockFailed => PyRuntimeError::new_err(self.to_string()),

            Self::CastArrayLengthMismatch
            | Self::ArrowChunkMissingField(_)
            | Self::DataframeInvalidH3IndexType(_, _)
            | Self::DataframeMissingColumn(_)
            | Self::UnsupportedH3Resolution(_)
            | Self::MixedH3Resolutions
            | Self::EmptyCells
            | Self::MissingQueryPlaceholder(_)
            | Self::SchemaValidationError(_, _)
            | Self::NoH3ResolutionsDefined
            | Self::MissingIndexValue => PyValueError::new_err(self.to_string()),

            Self::Abort => PyKeyboardInterrupt::new_err(self.to_string()),
        }
    }
}

impl ToCustomPyErr for ukis_h3cellstore::export::ukis_clickhouse_arrow_grpc::Error {
    fn to_custom_pyerr(self) -> PyErr {
        match self {
            Self::Polars(e) => e.to_custom_pyerr(),
            Self::Arrow(e) => e.to_custom_pyerr(),
            Self::TonicStatus(status) => status.to_custom_pyerr(),
            Self::ClickhouseException(ce) => ce.to_custom_pyerr(),
            Self::JoinError(e) => e.to_custom_pyerr(),
            Self::CastArrayLengthMismatch | Self::ArrowChunkMissingField(_) => {
                PyValueError::new_err(self.to_string())
            }
        }
    }
}

/// convert the result of some other crate into a PyResult
pub trait IntoPyResult<T> {
    fn into_pyresult(self) -> PyResult<T>;
}

impl<T, E> IntoPyResult<T> for Result<T, E>
where
    E: ToCustomPyErr,
{
    fn into_pyresult(self) -> PyResult<T> {
        match self {
            Ok(v) => Ok(v),
            Err(err) => Err(err.to_custom_pyerr()),
        }
    }
}

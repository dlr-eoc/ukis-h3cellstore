use h3cellstore::export::arrow_h3::export::h3ron;
use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};
use pyo3::PyResult;

/// convert the result of some other crate into a PyResult
pub trait IntoPyResult<T> {
    fn into_pyresult(self) -> PyResult<T>;
}

impl<T> IntoPyResult<T> for Result<T, h3cellstore::Error> {
    fn into_pyresult(self) -> PyResult<T> {
        use h3cellstore::Error;
        match self {
            Ok(v) => Ok(v),
            Err(err) => match err {
                Error::Polars(_) | Error::H3ron(_) | Error::JoinError(_) | Error::Arrow(_) => {
                    Err(PyRuntimeError::new_err(err.to_string()))
                }

                Error::TonicStatus(_)
                | Error::ClickhouseException { .. }
                | Error::MissingPrecondidtionsForPartialOptimization
                | Error::TableSetNotFound(_)
                | Error::NoQueryableTables => Err(PyIOError::new_err(err.to_string())),

                Error::CastArrayLengthMismatch
                | Error::ArrowChunkMissingField(_)
                | Error::DataframeInvalidH3IndexType(_, _)
                | Error::DataframeMissingColumn(_)
                | Error::UnsupportedH3Resolution(_)
                | Error::MixedH3Resolutions
                | Error::EmptyCells
                | Error::MissingQueryPlaceholder(_)
                | Error::SchemaValidationError(_, _)
                | Error::NoH3ResolutionsDefined
                | Error::MissingIndexValue => Err(PyValueError::new_err(err.to_string())),
            },
        }
    }
}

impl<T> IntoPyResult<T> for std::io::Result<T> {
    fn into_pyresult(self) -> PyResult<T> {
        match self {
            Ok(v) => Ok(v),
            Err(err) => Err(PyIOError::new_err(format!("IO error: {}", err.to_string()))),
        }
    }
}

impl<T> IntoPyResult<T> for Result<T, h3ron::Error> {
    fn into_pyresult(self) -> PyResult<T> {
        match self {
            Ok(v) => Ok(v),
            // TODO: more fine-grained mapping to python exceptions
            Err(err) => Err(PyValueError::new_err(err.to_string())),
        }
    }
}

impl<T> IntoPyResult<T> for Result<T, tokio::task::JoinError> {
    fn into_pyresult(self) -> PyResult<T> {
        match self {
            Ok(v) => Ok(v),
            Err(err) => Err(PyRuntimeError::new_err(format!(
                "joining task failed: {}",
                err.to_string()
            ))),
        }
    }
}

impl<T> IntoPyResult<T> for serde_json::Result<T> {
    fn into_pyresult(self) -> PyResult<T> {
        match self {
            Ok(v) => Ok(v),
            Err(err) => Err(PyIOError::new_err(format!(
                "JSON (de-)serializaiton failed: {}",
                err.to_string()
            ))),
        }
    }
}

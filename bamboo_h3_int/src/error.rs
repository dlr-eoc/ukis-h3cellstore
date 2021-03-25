use std::fmt;

use h3ron::Index;

#[derive(Debug)]
pub enum Error {
    EmptyIndexes,
    InvalidH3Index(Index),
    MixedResolutions,
    NoQueryableTables,
    MissingQueryPlaceholder(String),
    DifferentColumnLength(String, usize, usize),
    SchemaValidationError(&'static str, String),
    SerializationError(String)
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::EmptyIndexes => write!(f, "empty h3indexes"),
            Error::InvalidH3Index(index) => write!(f, "invalid h3indexes: {}", index.h3index()),
            Error::MixedResolutions => write!(f, "h3indexes with mixed resolutions"),
            Error::NoQueryableTables => write!(f, "no queryable tables found"),
            Error::MissingQueryPlaceholder(placeholder) => {
                write!(f, "missing query placeholder: {}", placeholder)
            },
            Error::DifferentColumnLength(column_name, expected_len, found_len) => {
                write!(f, "column {} has the a differing length. Expected {}, found {}", column_name, expected_len, found_len)
            },
            Error::SchemaValidationError(location, msg) => write!(f, "failed to validate {}: {}", location, msg),
            Error::SerializationError(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for Error {}

#[inline]
pub(crate) fn check_index_valid(index: &Index) -> std::result::Result<(), Error> {
    if !index.is_valid() {
        Err(Error::InvalidH3Index(index.clone()))
    } else {
        Ok(())
    }
}


impl From<serde_cbor::Error> for Error {
    fn from(se: serde_cbor::Error) -> Self {
        Error::SerializationError(format!("cbor serialization failed: {:?}", se))
    }
}

impl From<serde_json::error::Error> for Error {
    fn from(te: serde_json::error::Error) -> Self {
        Error::SerializationError(format!("{:?}",te))
    }
}

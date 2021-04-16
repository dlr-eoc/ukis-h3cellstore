use std::convert::TryFrom;
use std::fmt;

use h3ron::{HasH3Index, Index};

#[derive(Debug)]
pub enum Error {
    EmptyIndexes,
    InvalidH3Index(Index),
    MixedResolutions,
    NoQueryableTables,
    MissingQueryPlaceholder(String),
    DifferentColumnLength(String, usize, usize),
    SchemaValidationError(&'static str, String),
    SerializationError(String),
    InvalidH3Resolution(u8),
    UnknownDatatype(String),
    H3ron(h3ron::Error),
    Clickhouse(clickhouse_rs::errors::Error),
    ColumnNotFound(String),
    InvalidColumn(String),
    IncompatibleDatatype,
    UrlParseError(url::ParseError),
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
            }
            Error::DifferentColumnLength(column_name, expected_len, found_len) => {
                write!(
                    f,
                    "column {} has the a differing length. Expected {}, found {}",
                    column_name, expected_len, found_len
                )
            }
            Error::SchemaValidationError(location, msg) => {
                write!(f, "failed to validate {}: {}", location, msg)
            }
            Error::SerializationError(msg) => write!(f, "{}", msg),
            Error::InvalidH3Resolution(res) => write!(f, "invalid h3 resolution: {}", res),
            Error::UnknownDatatype(dt) => write!(f, "unknown datatype: {}", dt),
            Error::H3ron(e) => write!(f, "h3ron: {}", e),
            Error::ColumnNotFound(column_name) => write!(f, "column not found: {}", column_name),
            Error::InvalidColumn(column_name) => write!(f, "invalid column: {}", column_name),
            Error::Clickhouse(e) => write!(f, "clickhouse: {:?}", e),
            Error::IncompatibleDatatype => write!(f, "incompatible datatype"),
            Error::UrlParseError(upe) => write!(f, "unable to parse url: {:?}", upe),
        }
    }
}

impl std::error::Error for Error {}

#[inline]
pub(crate) fn check_index_valid(index: &Index) -> std::result::Result<(), Error> {
    index
        .validate()
        .map_err(|_| Error::InvalidH3Index(*index))
}

pub(crate) fn check_same_h3_resolution<T: HasH3Index>(
    indexes: &[T],
) -> std::result::Result<(), Error> {
    if let Some(first) = indexes.get(0) {
        let first_index = Index::try_from(first.h3index())?;
        let expected_res = first_index.resolution();
        for idx in indexes.iter() {
            let this_index = Index::try_from(idx.h3index())?;
            if this_index.resolution() != expected_res {
                return Err(Error::MixedResolutions);
            }
        }
    }
    Ok(())
}

impl From<serde_cbor::Error> for Error {
    fn from(se: serde_cbor::Error) -> Self {
        Error::SerializationError(format!("cbor serialization failed: {:?}", se))
    }
}

impl From<serde_json::error::Error> for Error {
    fn from(te: serde_json::error::Error) -> Self {
        Error::SerializationError(format!("{:?}", te))
    }
}

impl From<h3ron::Error> for Error {
    fn from(e: h3ron::Error) -> Self {
        Error::H3ron(e)
    }
}

impl From<clickhouse_rs::errors::Error> for Error {
    fn from(e: clickhouse_rs::errors::Error) -> Self {
        Error::Clickhouse(e)
    }
}

impl From<url::ParseError> for Error {
    fn from(e: url::ParseError) -> Self {
        Error::UrlParseError(e)
    }
}

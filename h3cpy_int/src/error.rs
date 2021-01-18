use std::fmt;

use h3ron::Index;

#[derive(Debug)]
pub enum Error {
    EmptyIndexes,
    InvalidH3Index(Index),
    MixedResolutions,
    NoQueryableTables,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::EmptyIndexes => write!(f, "empty h3indexes"),
            Error::InvalidH3Index(index) => write!(f, "invalid h3indexes: {}", index.h3index()),
            Error::MixedResolutions => write!(f, "h3indexes with mixed resolutions"),
            Error::NoQueryableTables => write!(f, "no queryable tables found")
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

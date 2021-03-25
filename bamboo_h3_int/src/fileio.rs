use std::io;

use crate::error::Error;
use serde::Serialize;
use zstd::{Decoder, Encoder};

pub fn serialize_into<W, T: ?Sized>(writer: W, value: &T) -> Result<(), Error>
where
    W: io::Write,
    T: Serialize + Sized,
{
    // level was chosen based on https://gitlab.dlr.de/gzs-processing/processing-results-to-h3/-/issues/15#note_650119
    let encoder = Encoder::new(writer, 6)
        .map_err(|e| Error::CompressionError(format!("could not zstd endcode: {:?}", e)))?
        .auto_finish();
    serde_cbor::to_writer(encoder, value).map_err(|e| e.into())
}

pub fn deserialize_from<R, T>(reader: R) -> Result<T, Error>
where
    R: io::Read + io::Seek,
    T: serde::de::DeserializeOwned + Sized,
{
    let decoder = Decoder::new(reader)
        .map_err(|e| Error::CompressionError(format!("could not zstd decode: {:?}", e)))?;
    serde_cbor::from_reader(decoder).map_err(|e| e.into())
}

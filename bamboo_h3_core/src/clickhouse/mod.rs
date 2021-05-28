use std::collections::HashMap;

use clickhouse_rs::Block;
use tracing::{warn, error};
use url::Url;

use crate::error::Error;
use crate::{ColVec, ColumnSet, Datatype};
use std::time::Duration;

pub mod compacted_tables;
pub mod query;
pub mod schema;
pub mod walk;

/** convert to a block including some type conversions */
pub trait FromWithDatatypes<T> {
    fn from_with_datatypes(
        cs: ColumnSet,
        target_typemap: &HashMap<String, Datatype>,
    ) -> Result<Self, Error>
    where
        Self: Sized;
}

impl FromWithDatatypes<ColumnSet> for Block {
    fn from_with_datatypes(
        mut cs: ColumnSet,
        target_typemap: &HashMap<String, Datatype>,
    ) -> Result<Self, Error> {
        let mut block = Self::with_capacity(cs.len());
        for (column_name, colvec) in cs.columns.drain() {
            let target_type = if let Some(dt) = target_typemap.get(&column_name) {
                dt
            } else {
                return Err(Error::ColumnNotFound(column_name));
            };
            let colvec_datatype = colvec.datatype().clone();
            block = match (colvec, target_type) {
                (ColVec::U8(v), Datatype::U8) => block.column(&column_name, v),
                (ColVec::U8N(v), Datatype::U8N) => block.column(&column_name, v),
                (ColVec::I8(v), Datatype::I8) => block.column(&column_name, v),
                (ColVec::I8N(v), Datatype::I8N) => block.column(&column_name, v),
                (ColVec::U16(v), Datatype::U16) => block.column(&column_name, v),
                (ColVec::U16N(v), Datatype::U16N) => block.column(&column_name, v),
                (ColVec::I16(v), Datatype::I16) => block.column(&column_name, v),
                (ColVec::I16N(v), Datatype::I16N) => block.column(&column_name, v),
                (ColVec::U32(v), Datatype::U32) => block.column(&column_name, v),
                (ColVec::U32N(v), Datatype::U32N) => block.column(&column_name, v),
                (ColVec::I32(v), Datatype::I32) => block.column(&column_name, v),
                (ColVec::I32N(v), Datatype::I32N) => block.column(&column_name, v),
                (ColVec::U64(v), Datatype::U64) => block.column(&column_name, v),
                (ColVec::U64N(v), Datatype::U64N) => block.column(&column_name, v),
                (ColVec::I64(v), Datatype::I64) => block.column(&column_name, v),
                (ColVec::I64N(v), Datatype::I64N) => block.column(&column_name, v),
                (ColVec::F32(v), Datatype::F32) => block.column(&column_name, v),
                (ColVec::F32N(v), Datatype::F32N) => block.column(&column_name, v),
                (ColVec::F64(v), Datatype::F64) => block.column(&column_name, v),
                (ColVec::F64N(v), Datatype::F64N) => block.column(&column_name, v),
                (ColVec::Date(v), Datatype::Date) => block.column(&column_name, v),
                (ColVec::DateN(v), Datatype::DateN) => block.column(&column_name, v),
                (ColVec::DateTime(v), Datatype::DateTime) => block.column(&column_name, v),
                (ColVec::DateTime(mut v), Datatype::Date) => {
                    let dates: Vec<_> = v.drain(..).map(|dt| dt.date()).collect();
                    block.column(&column_name, dates)
                }
                (ColVec::DateTimeN(mut v), Datatype::DateTimeN) => {
                    let dates: Vec<_> = v.drain(..).map(|o| o.map(|dt| dt.date())).collect();
                    block.column(&column_name, dates)
                }
                (_, _) => {
                    error!(
                        "colvec typed {} can not be converted to {} typed block column",
                        colvec_datatype,
                        target_type
                    );
                    return Err(Error::IncompatibleDatatype);
                }
            };
        }
        Ok(block)
    }
}

pub fn validate_clickhouse_url(
    u: &str,
    default_connection_timeout_ms: Option<u32>,
) -> Result<String, Error> {
    let mut parsed_url = Url::parse(u)?;

    let parameters: HashMap<_, _> = parsed_url
        .query_pairs()
        .map(|(name, value)| (name.to_lowercase(), value.to_string()))
        .collect();

    if parameters
        .get("compression")
        .cloned()
        .unwrap_or_else(|| "none".to_string())
        == *"none"
    {
        warn!("possible inefficient data transfer: consider setting a compression_method in the clickhouse connection parameters. 'lz4' is one option.")
    }

    if parameters.get("connection_timeout").is_none() {
        if let Some(default_ct) = default_connection_timeout_ms {
            let mut qp = parsed_url.query_pairs_mut();
            qp.append_pair("connection_timeout", &format!("{}ms", default_ct));
        } else {
            warn!("short connection_timeout: clickhouse connection parameters sets no connection_timeout, so it uses the very short default of 500ms")
        }
    }

    Ok(parsed_url.into_string())
}

pub struct QueryOutput<T> {
    pub data: T,

    /// the indexes queried from the DB
    pub h3indexes_queried: Option<Vec<u64>>,

    /// In case of using `CellWalk`, the h3index of the cell
    pub containing_h3index: Option<u64>,

    /// the duration the query took to finish
    pub query_duration: Option<Duration>,
}

#[cfg(test)]
mod tests {
    use crate::clickhouse::validate_clickhouse_url;

    #[test]
    fn test_clickhouse_url_append_connection_timeout() {
        let validated =
            validate_clickhouse_url("tcp://localhost:9010/water2?compression=lz4", Some(2000))
                .unwrap();
        assert!(validated.contains("connection_timeout=2000ms"));
    }
}

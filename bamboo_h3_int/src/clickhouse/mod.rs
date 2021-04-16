use crate::{ColVec, ColumnSet};
use clickhouse_rs::Block;
use std::collections::HashMap;
use log::warn;
use url::Url;
use crate::error::Error;
pub mod compacted_tables;
pub mod query;
pub mod schema;
pub mod window;

impl From<ColumnSet> for Block {
    fn from(mut cs: ColumnSet) -> Self {
        let mut block = Self::with_capacity(cs.len());
        for (column_name, colvec) in cs.columns.drain() {
            block = match colvec {
                ColVec::U8(v) => block.column(&column_name, v),
                ColVec::U8N(v) => block.column(&column_name, v),
                ColVec::I8(v) => block.column(&column_name, v),
                ColVec::I8N(v) => block.column(&column_name, v),
                ColVec::U16(v) => block.column(&column_name, v),
                ColVec::U16N(v) => block.column(&column_name, v),
                ColVec::I16(v) => block.column(&column_name, v),
                ColVec::I16N(v) => block.column(&column_name, v),
                ColVec::U32(v) => block.column(&column_name, v),
                ColVec::U32N(v) => block.column(&column_name, v),
                ColVec::I32(v) => block.column(&column_name, v),
                ColVec::I32N(v) => block.column(&column_name, v),
                ColVec::U64(v) => block.column(&column_name, v),
                ColVec::U64N(v) => block.column(&column_name, v),
                ColVec::I64(v) => block.column(&column_name, v),
                ColVec::I64N(v) => block.column(&column_name, v),
                ColVec::F32(v) => block.column(&column_name, v),
                ColVec::F32N(v) => block.column(&column_name, v),
                ColVec::F64(v) => block.column(&column_name, v),
                ColVec::F64N(v) => block.column(&column_name, v),
                ColVec::Date(v) => block.column(&column_name, v),
                ColVec::DateN(v) => block.column(&column_name, v),
                ColVec::DateTime(v) => block.column(&column_name, v),
                ColVec::DateTimeN(v) => block.column(&column_name, v),
            };
        }
        block
    }
}


pub fn validate_clickhouse_url(u: &str, default_connection_timeout_ms: Option<u32>) -> Result<String, Error> {
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
        if let Some(default_ct) = default_connection_timeout_ms  {
            let mut qp = parsed_url.query_pairs_mut();
            qp.append_pair("connection_timeout", &format!("{}ms", default_ct));
        } else {
            warn!("short connection_timeout: clickhouse connection parameters sets no connection_timeout, so it uses the very short default of 500ms")
        }
    }

    Ok(parsed_url.into_string())
}


#[cfg(test)]
mod tests {
    use crate::clickhouse::validate_clickhouse_url;

    #[test]
    fn test_clickhouse_url_append_connection_timeout() {
        let validated = validate_clickhouse_url("tcp://localhost:9010/water2?compression=lz4", Some(2000)).unwrap();
        assert!(validated.contains("connection_timeout=2000ms"));
    }
}

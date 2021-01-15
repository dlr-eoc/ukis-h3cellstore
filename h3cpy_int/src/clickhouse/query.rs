use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use chrono::prelude::*;
use chrono_tz::Tz;
use clickhouse_rs::{
    ClientHandle,
    errors::{Error, Result},
    types::SqlType,
};
use futures_util::StreamExt;
use h3ron::Index;
use log::{error, warn};

use crate::ColVec;
use crate::compacted_tables::{
    find_tablesets,
    TableSet,
};

/// list all tablesets in the current database
pub async fn list_tablesets(mut ch: ClientHandle) -> Result<HashMap<String, TableSet>> {
    let mut tablesets = {
        let mut stream = ch.query("select table
                from system.columns
                where name = 'h3index' and database = currentDatabase()"
        ).stream();

        let mut tablenames = vec![];
        while let Some(row_res) = stream.next().await {
            let row = row_res?;
            let tablename: String = row.get("table")?;
            tablenames.push(tablename);
        }
        find_tablesets(&tablenames)
    };

    // find the columns for the tablesets
    for (ts_name, ts) in tablesets.iter_mut() {
        let set_table_names = itertools::join(
            ts.tables()
                .iter()
                .map(|t| format!("'{}'", t.to_table_name()))
            , ", ");

        let mut columns_stream = ch.query(format!("
            select name, type, count(*) as c
                from system.columns
                where table in ({})
                and database = currentDatabase()
                and not startsWith(name, 'h3index')
                group by name, type
        ", set_table_names)).stream();
        while let Some(c_row_res) = columns_stream.next().await {
            let c_row = c_row_res?;
            let c: u64 = c_row.get("c")?;
            let col_name: String = c_row.get("name")?;

            // column must be present in all tables of the set, or it is not usable
            if c as usize == ts.num_tables() {
                let col_type: String = c_row.get("type")?;
                ts.columns.insert(col_name, col_type);
            } else {
                warn!("column {} is not present using the same type in all tables of set {}. ignoring the column", col_name, ts_name);
            }
        }
    }

    Ok(tablesets)
}

/// check if a query would return any rows
pub async fn query_returns_rows(mut ch: ClientHandle, query_string: String) -> Result<bool> {
    let mut stream = ch.query(query_string).stream();
    if let Some(first) = stream.next().await {
        match first {
            Ok(_) => Ok(true),
            Err(e) => Err(e)
        }
    } else {
        Ok(false)
    }
}

pub async fn query_all(mut ch: ClientHandle, query_string: String) -> Result<HashMap<String, ColVec>> {
    let block = ch.query(query_string).fetch_all().await?;

    let mut out_rows = HashMap::new();
    for column in block.columns() {
        // TODO: how to handle nullable columns?
        // TODO: move column data without cloning
        let values = match column.sql_type() {
            SqlType::UInt8 => ColVec::U8(column.iter::<u8>()?.cloned().collect()),
            SqlType::UInt16 => ColVec::U16(column.iter::<u16>()?.cloned().collect()),
            SqlType::UInt32 => ColVec::U32(column.iter::<u32>()?.cloned().collect()),
            SqlType::UInt64 => ColVec::U64(column.iter::<u64>()?.cloned().collect()),
            SqlType::Int8 => ColVec::I8(column.iter::<i8>()?.cloned().collect()),
            SqlType::Int16 => ColVec::I16(column.iter::<i16>()?.cloned().collect()),
            SqlType::Int32 => ColVec::I32(column.iter::<i32>()?.cloned().collect()),
            SqlType::Int64 => ColVec::I64(column.iter::<i64>()?.cloned().collect()),
            SqlType::Float32 => ColVec::F32(column.iter::<f32>()?.cloned().collect()),
            SqlType::Float64 => ColVec::F64(column.iter::<f64>()?.cloned().collect()),
            SqlType::Date => {
                let u = column.iter::<Date<Tz>>()?
                    .map(|d| d.and_hms(12, 0, 0).timestamp())
                    .collect();
                ColVec::Date(u)
            }
            SqlType::DateTime(_) => {
                let u = column.iter::<DateTime<Tz>>()?
                    .map(|d| d.timestamp())
                    .collect();
                ColVec::DateTime(u)
            }
            _ => {
                error!("unsupported column type {} for column {}", column.sql_type().to_string(), column.name());
                return Err(Error::Other(Cow::from("unsupported column type")));
            }
        };
        out_rows.insert(column.name().to_string(), values);
    }
    Ok(out_rows)
}

/// return all rows from the query and uncompact the h3index in the h3index column, all other columns get duplicated accordingly
pub async fn query_all_with_uncompacting(mut ch: ClientHandle, query_string: String, h3index_set: HashSet<u64>) -> Result<HashMap<String, ColVec>> {
    let h3_res = if let Some(first) = h3index_set.iter().next() {
        Index::from(*first).resolution()
    } else {
        return Err(Error::Other(Cow::from("no h3indexes given")));
    };
    let block = ch.query(query_string).fetch_all().await?;

    let h3index_column = if let Some(c) = block.columns().iter()
        .find(|c| c.name() == "h3index") {
        c
    } else {
        return Err(Error::Other(Cow::from("no h3index column found")));
    };

    // the number denoting how often a value of the other columns must be repeated
    // to match the number of uncompacted h3indexes
    let mut row_repetitions = Vec::with_capacity(block.row_count());

    // uncompact the h3index columns contents
    let (h3_vec, num_uncompacted_rows) = {
        let mut h3_vec = Vec::new();
        for h3index in h3index_column.iter::<u64>()? {
            let idx = Index::from(*h3index);
            let m = if idx.resolution() < h3_res {
                let mut valid_children = idx.get_children(h3_res)
                    .drain(..)
                    .map(|i| i.h3index())
                    .filter(|hi| h3index_set.contains(hi))
                    .collect::<Vec<_>>();
                let m = valid_children.len();
                h3_vec.append(&mut valid_children);
                m
            } else if idx.resolution() == h3_res {
                h3_vec.push(idx.h3index());
                1
            } else {
                return Err(Error::Other(Cow::from("too small resolution during uncompacting")));
            };
            row_repetitions.push(m);
        }
        let num_uncompacted_rows = h3_vec.len();
        (ColVec::U64(h3_vec), num_uncompacted_rows)
    };

    let mut out_rows = HashMap::new();
    for column in block.columns() {
        if column.name() == "h3index" {
            continue;
        }

        /// repeat column values according to the counts of the row_repetitions vec
        macro_rules! repeat_column_values {
            ($cvtype:ident, $itertype:ty, $conv_closure:expr) => {
            {
                let mut values = Vec::with_capacity(num_uncompacted_rows);
                let mut pos = 0_usize;
                for v in column.iter::<$itertype>()?.map($conv_closure) {
                    for _i in 0..row_repetitions[pos] {
                        // TODO: move column data without cloning?
                        values.push(v.clone())
                    }
                    pos += 1;
                };
                ColVec::$cvtype(values)
            }
            };
            ($cvtype:ident, $itertype:ty) => {
                 repeat_column_values!($cvtype, $itertype, |v| v)
            };
        }
        // TODO: how to handle nullable columns?
        let values = match column.sql_type() {
            SqlType::UInt8 => repeat_column_values!(U8, u8),
            SqlType::UInt16 => repeat_column_values!(U16, u16),
            SqlType::UInt32 => repeat_column_values!(U32, u32),
            SqlType::UInt64 => repeat_column_values!(U64, u64),
            SqlType::Int8 => repeat_column_values!(I8, i8),
            SqlType::Int16 => repeat_column_values!(I16, i16),
            SqlType::Int32 => repeat_column_values!(I32, i32),
            SqlType::Int64 => repeat_column_values!(I64, i64),
            SqlType::Float32 => repeat_column_values!(F32, f32),
            SqlType::Float64 => repeat_column_values!(F64, f64),
            SqlType::Date => repeat_column_values!(Date, Date<Tz>, |d| d.and_hms(12, 0, 0).timestamp()),
            SqlType::DateTime(_) => repeat_column_values!(DateTime, DateTime<Tz>, |d| d.timestamp()),
            _ => {
                error!("unsupported column type {} for column {}", column.sql_type().to_string(), column.name());
                return Err(Error::Other(Cow::from("unsupported column type")));
            }
        };
        out_rows.insert(column.name().to_string(), values);
    }
    out_rows.insert("h3index".to_string(), h3_vec);
    Ok(out_rows)
}

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use chrono::prelude::*;
use chrono_tz::Tz;
use clickhouse_rs::{
    ClientHandle,
    errors::{Error, Result},
    types::SqlType,
};
use clickhouse_rs::types::{Column, Complex};
use futures_util::StreamExt;
use h3ron::Index;
use log::{error, warn};

use crate::ColVec;
use crate::compacted_tables::{find_tablesets, TableSet};

/// list all tablesets in the current database
pub async fn list_tablesets(mut ch: ClientHandle) -> Result<HashMap<String, TableSet>> {
    let mut tablesets = {
        let mut stream = ch
            .query(
                "select table
                from system.columns
                where name = 'h3index' and database = currentDatabase()",
            )
            .stream();

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
                .map(|t| format!("'{}'", t.to_table_name())),
            ", ",
        );

        let mut columns_stream = ch
            .query(format!(
                "
            select name, type, count(*) as c
                from system.columns
                where table in ({})
                and database = currentDatabase()
                and not startsWith(name, 'h3index')
                group by name, type
        ",
                set_table_names
            ))
            .stream();
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
            Err(e) => Err(e),
        }
    } else {
        Ok(false)
    }
}

pub async fn query_all(
    mut ch: ClientHandle,
    query_string: String,
) -> Result<HashMap<String, ColVec>> {
    let block = ch.query(query_string).fetch_all().await?;

    let mut out_rows = HashMap::new();
    for column in block.columns() {
        out_rows.insert(
            column.name().to_string(),
            read_column(column, None)?,
        );
    }
    Ok(out_rows)
}

/// return all rows from the query and uncompact the h3index in the h3index column, all other columns get duplicated accordingly
pub async fn query_all_with_uncompacting(
    mut ch: ClientHandle,
    query_string: String,
    h3index_set: HashSet<u64>,
) -> Result<HashMap<String, ColVec>> {
    let h3_res = if let Some(first) = h3index_set.iter().next() {
        Index::from(*first).resolution()
    } else {
        return Err(Error::Other(Cow::from("no h3indexes given")));
    };
    let block = ch.query(query_string).fetch_all().await?;

    let h3index_column = if let Some(c) = block.columns().iter().find(|c| c.name() == "h3index") {
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
                let mut valid_children = idx
                    .get_children(h3_res)
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
                return Err(Error::Other(Cow::from(
                    "too small resolution during uncompacting",
                )));
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

        out_rows.insert(
            column.name().to_string(),
            read_column(column, Some((num_uncompacted_rows, &row_repetitions)))?,
        );
    }
    out_rows.insert("h3index".to_string(), h3_vec);
    Ok(out_rows)
}

fn read_column(column: &Column<Complex>, row_reps: Option<(usize, &Vec<usize>)>) -> Result<ColVec> {
    macro_rules! column_values {
            ($cvtype:ident, $itertype:ty, $conv_closure:expr) => {{
                let values = if let Some((num_uncompacted_rows, row_repetitions)) = row_reps {
                    // repeat columns values according to the counts of the row_repetitions vec
                    // to create a "flat" table.
                    let mut values = Vec::with_capacity(num_uncompacted_rows);
                    let mut pos = 0_usize;
                    for v in column.iter::<$itertype>()?.map($conv_closure) {
                        for _ in 0..row_repetitions[pos] {
                            values.push(v)
                        }
                        pos += 1;
                    }
                    values
                } else {
                    // just copy everything
                    column.iter::<$itertype>()?.map($conv_closure).collect()
                };
                ColVec::$cvtype(values)
            }};
            ($cvtype:ident, $itertype:ty) => {
                if row_reps.is_some() {
                    column_values!($cvtype, $itertype, |v| v.clone())
                } else {
                    let values = column.iter::<$itertype>()?.cloned().collect::<Vec<_>>();
                    ColVec::$cvtype(values)
                }
            };
        }

    let values = match column.sql_type() {
        SqlType::UInt8 => column_values!(U8, u8),
        SqlType::UInt16 => column_values!(U16, u16),
        SqlType::UInt32 => column_values!(U32, u32),
        SqlType::UInt64 => column_values!(U64, u64),
        SqlType::Int8 => column_values!(I8, i8),
        SqlType::Int16 => column_values!(I16, i16),
        SqlType::Int32 => column_values!(I32, i32),
        SqlType::Int64 => column_values!(I64, i64),
        SqlType::Float32 => column_values!(F32, f32),
        SqlType::Float64 => column_values!(F64, f64),
        SqlType::Date => {
            column_values!(Date, Date<Tz>, |d| to_datetime(&d).timestamp())
        }
        SqlType::DateTime(_) => {
            column_values!(DateTime, DateTime<Tz>, |d| d.timestamp())
        }
        SqlType::Nullable(inner_sqltype) => {
            match inner_sqltype {
                SqlType::UInt8 => column_values!(U8N, Option<u8>, |v| v.map(|inner| inner.clone())),
                SqlType::UInt16 => column_values!(U16N, Option<u16>, |v| v.map(|inner| inner.clone())),
                SqlType::UInt32 => column_values!(U32N, Option<u32>, |v| v.map(|inner| inner.clone())),
                SqlType::UInt64 => column_values!(U64N, Option<u64>, |v| v.map(|inner| inner.clone())),
                SqlType::Int8 => column_values!(I8N, Option<i8>, |v| v.map(|inner| inner.clone())),
                SqlType::Int16 => column_values!(I16N, Option<i16>, |v| v.map(|inner| inner.clone())),
                SqlType::Int32 => column_values!(I32N, Option<i32>, |v| v.map(|inner| inner.clone())),
                SqlType::Int64 => column_values!(I64N, Option<i64>, |v| v.map(|inner| inner.clone())),
                SqlType::Float32 => column_values!(F32N, Option<f32>, |v| v.map(|inner| inner.clone())),
                SqlType::Float64 => column_values!(F64N, Option<f64>, |v| v.map(|inner| inner.clone())),
                SqlType::Date => {
                    column_values!(DateN, Option<Date<Tz>>, |d| d.map(|inner| to_datetime(&inner).timestamp()))
                }
                SqlType::DateTime(_) => {
                    column_values!(DateTimeN, Option<DateTime<Tz>>, |d| d.map(|inner| inner.timestamp()))
                }
                _ => {
                    error!(
                        "unsupported nullable column type {} for column {}",
                        column.sql_type().to_string(),
                        column.name()
                    );
                    return Err(Error::Other(Cow::from("unsupported nullable column type")));
                }
            }
        }
        _ => {
            error!(
                "unsupported column type {} for column {}",
                column.sql_type().to_string(),
                column.name()
            );
            return Err(Error::Other(Cow::from("unsupported column type")));
        }
    };
    Ok(values)
}


#[inline]
fn to_datetime(date: &Date<Tz>) -> DateTime<Tz> {
    date.and_hms(12, 0 ,0)
}
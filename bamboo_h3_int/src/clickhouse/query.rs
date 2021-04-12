use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::iter::FromIterator;

use chrono::prelude::*;
use chrono_tz::Tz;
use clickhouse_rs::types::{Column, Complex};
use clickhouse_rs::{types::SqlType, ClientHandle};
use futures_util::StreamExt;
use h3ron::{HasH3Index, Index};
use log::{error, warn};

use crate::clickhouse::compacted_tables::{find_tablesets, TableSet};
use crate::error::Error;
use crate::iter::ItemRepeatingIterator;
use crate::{ColVec, ColumnSet, COL_NAME_H3INDEX};

/// list all tablesets in the current database
pub async fn list_tablesets(mut ch: ClientHandle) -> Result<HashMap<String, TableSet>, Error> {
    let mut tablesets = {
        let mut stream = ch
            .query(format!(
                "select table
                from system.columns
                where name = '{}' and database = currentDatabase()",
                COL_NAME_H3INDEX
            ))
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
                and not startsWith(name, '{}')
                group by name, type
        ",
                set_table_names, COL_NAME_H3INDEX
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
pub async fn query_returns_rows(mut ch: ClientHandle, query_string: String) -> Result<bool, Error> {
    let mut stream = ch.query(query_string).stream();
    if let Some(first) = stream.next().await {
        match first {
            Ok(_) => Ok(true),
            Err(e) => Err(e.into()),
        }
    } else {
        Ok(false)
    }
}

pub async fn query_all(mut ch: ClientHandle, query_string: String) -> Result<ColumnSet, Error> {
    let block = ch.query(query_string).fetch_all().await?;

    let mut out_rows = HashMap::new();
    for column in block.columns() {
        out_rows.insert(column.name().to_string(), read_column(column, None)?);
    }
    Ok(out_rows.into())
}

/// return all rows from the query and uncompact the h3index in the h3index column, all other columns get duplicated accordingly
pub async fn query_all_with_uncompacting(
    mut ch: ClientHandle,
    query_string: String,
    h3index_set: HashSet<u64>,
) -> Result<ColumnSet, Error> {
    let h3_res = if let Some(first) = h3index_set.iter().next() {
        let index = Index::try_from(*first)?;
        index.resolution()
    } else {
        return Err(Error::EmptyIndexes);
    };
    let block = ch.query(query_string).fetch_all().await?;

    let h3index_column = if let Some(c) = block
        .columns()
        .iter()
        .find(|c| c.name() == COL_NAME_H3INDEX)
    {
        c
    } else {
        return Err(Error::ColumNotFound(COL_NAME_H3INDEX.to_string()));
    };

    // the number denoting how often a value of the other columns must be repeated
    // to match the number of uncompacted h3indexes
    let mut row_repetitions = Vec::with_capacity(block.row_count());

    // uncompact the h3index columns contents
    let (h3_vec, num_uncompacted_rows) = {
        let mut h3_vec = Vec::new();
        for h3index in h3index_column.iter::<u64>()? {
            let idx: Index = Index::try_from(*h3index)?;
            let m = match idx.resolution().cmp(&h3_res) {
                Ordering::Less => {
                    let mut valid_children = idx
                        .get_children(h3_res)
                        .drain(..)
                        .map(|i| i.h3index())
                        .filter(|hi| h3index_set.contains(hi))
                        .collect::<Vec<_>>();
                    let m = valid_children.len();
                    h3_vec.append(&mut valid_children);
                    m
                }
                Ordering::Equal => {
                    h3_vec.push(idx.h3index());
                    1
                }
                _ => {
                    return Err(Error::InvalidH3Resolution(idx.resolution()));
                }
            };
            row_repetitions.push(m);
        }
        let num_uncompacted_rows = h3_vec.len();
        (ColVec::U64(h3_vec), num_uncompacted_rows)
    };

    let mut out_rows = HashMap::new();
    for column in block.columns() {
        if column.name() == COL_NAME_H3INDEX {
            continue;
        }

        out_rows.insert(
            column.name().to_string(),
            read_column(column, Some((num_uncompacted_rows, &row_repetitions)))?,
        );
    }
    out_rows.insert(COL_NAME_H3INDEX.to_string(), h3_vec);
    Ok(out_rows.into())
}

#[inline]
fn collect_with_reps<I, T, O>(iter: I, row_reps: Option<(usize, &Vec<usize>)>) -> O
where
    I: Iterator<Item = T>,
    T: Clone,
    O: FromIterator<T>,
{
    if let Some((num_uncompacted_rows, row_repetitions)) = row_reps {
        ItemRepeatingIterator::new(iter, &row_repetitions, Some(num_uncompacted_rows)).collect()
    } else {
        iter.collect()
    }
}

fn read_column(
    column: &Column<Complex>,
    row_reps: Option<(usize, &Vec<usize>)>,
) -> Result<ColVec, Error> {
    let values: ColVec = match column.sql_type() {
        SqlType::UInt8 => collect_with_reps(column.iter::<u8>()?.copied(), row_reps),
        SqlType::UInt16 => collect_with_reps(column.iter::<u16>()?.copied(), row_reps),
        SqlType::UInt32 => collect_with_reps(column.iter::<u32>()?.copied(), row_reps),
        SqlType::UInt64 => collect_with_reps(column.iter::<u64>()?.copied(), row_reps),
        SqlType::Int8 => collect_with_reps(column.iter::<i8>()?.copied(), row_reps),
        SqlType::Int16 => collect_with_reps(column.iter::<i16>()?.copied(), row_reps),
        SqlType::Int32 => collect_with_reps(column.iter::<i32>()?.copied(), row_reps),
        SqlType::Int64 => collect_with_reps(column.iter::<i64>()?.copied(), row_reps),
        SqlType::Float32 => collect_with_reps(column.iter::<f32>()?.copied(), row_reps),
        SqlType::Float64 => collect_with_reps(column.iter::<f64>()?.copied(), row_reps),
        SqlType::Date => collect_with_reps(column.iter::<Date<Tz>>()?, row_reps),
        SqlType::DateTime(_) => collect_with_reps(column.iter::<DateTime<Tz>>()?, row_reps),
        SqlType::Nullable(inner_sqltype) => match inner_sqltype {
            SqlType::UInt8 => {
                collect_with_reps(column.iter::<Option<u8>>()?.map(|v| v.copied()), row_reps)
            }
            SqlType::UInt16 => {
                collect_with_reps(column.iter::<Option<u16>>()?.map(|v| v.copied()), row_reps)
            }
            SqlType::UInt32 => {
                collect_with_reps(column.iter::<Option<u32>>()?.map(|v| v.copied()), row_reps)
            }
            SqlType::UInt64 => {
                collect_with_reps(column.iter::<Option<u64>>()?.map(|v| v.copied()), row_reps)
            }
            SqlType::Int8 => {
                collect_with_reps(column.iter::<Option<i8>>()?.map(|v| v.copied()), row_reps)
            }
            SqlType::Int16 => {
                collect_with_reps(column.iter::<Option<i16>>()?.map(|v| v.copied()), row_reps)
            }
            SqlType::Int32 => {
                collect_with_reps(column.iter::<Option<i32>>()?.map(|v| v.copied()), row_reps)
            }
            SqlType::Int64 => {
                collect_with_reps(column.iter::<Option<i64>>()?.map(|v| v.copied()), row_reps)
            }
            SqlType::Float32 => {
                collect_with_reps(column.iter::<Option<f32>>()?.map(|v| v.copied()), row_reps)
            }
            SqlType::Float64 => {
                collect_with_reps(column.iter::<Option<f64>>()?.map(|v| v.copied()), row_reps)
            }
            SqlType::Date => collect_with_reps(column.iter::<Option<Date<Tz>>>()?, row_reps),
            SqlType::DateTime(_) => {
                collect_with_reps(column.iter::<Option<DateTime<Tz>>>()?, row_reps)
            }
            _ => {
                error!(
                    "unsupported nullable column type {} for column {}",
                    column.sql_type().to_string(),
                    column.name()
                );
                return Err(Error::UnknownDatatype(
                    column.sql_type().to_string().to_string(),
                ));
            }
        },
        _ => {
            error!(
                "unsupported column type {} for column {}",
                column.sql_type().to_string(),
                column.name()
            );
            return Err(Error::UnknownDatatype(
                column.sql_type().to_string().to_string(),
            ));
        }
    };
    Ok(values)
}

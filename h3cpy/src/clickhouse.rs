use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use clickhouse_rs::{
    ClientHandle,
    errors::Error as ChError,
    errors::Result as ChResult,
    Pool,
};
use clickhouse_rs::types::SqlType;
use futures_util::StreamExt;
use log::{error, warn};
use pyo3::{PyErr, PyResult};
use pyo3::exceptions::PyRuntimeError;
use tokio::runtime::Runtime;
use chrono::prelude::*;
use chrono_tz::Tz;

use h3cpy_int::compacted_tables::find_tablesets;

use crate::inspect::TableSet as TableSetWrapper;
use h3ron::Index;

pub fn ch_to_pyerr(ch_err: ChError) -> PyErr {
    PyRuntimeError::new_err(format!("clickhouse error: {:?}", ch_err))
}

pub fn ch_to_pyresult<T>(res: ChResult<T>) -> PyResult<T> {
    match res {
        Ok(v) => Ok(v),
        Err(e) => Err(ch_to_pyerr(e))
    }
}

pub(crate) struct RuntimedPool {
    pub(crate) pool: Pool,
    pub(crate) rt: Runtime,
}

impl RuntimedPool {
    pub fn create(db_url: &str) -> PyResult<RuntimedPool> {
        let rt = match Runtime::new() {
            Ok(rt) => rt,
            Err(e) => return Err(PyRuntimeError::new_err(format!("could not create tokio rt: {:?}", e)))
        };
        Ok(Self {
            pool: Pool::new(db_url),
            rt,
        })
    }

    pub fn get_client(&mut self) -> PyResult<ClientHandle> {
        let p = &self.pool;
        ch_to_pyresult(self.rt.block_on(async { p.get_handle().await }))
    }
}

pub async fn list_tablesets(mut ch: ClientHandle) -> ChResult<HashMap<String, TableSetWrapper>> {
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

    Ok(tablesets
        .drain()
        .map(|(k, v)| (k, TableSetWrapper { inner: v }))
        .collect())
}

pub async fn query_returns_rows(mut ch: ClientHandle, query_string: String) -> ChResult<bool> {
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

pub enum ColVec {
    U8(Vec<u8>),
    I8(Vec<i8>),
    U16(Vec<u16>),
    I16(Vec<i16>),
    U32(Vec<u32>),
    I32(Vec<i32>),
    U64(Vec<u64>),
    I64(Vec<i64>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    /// unix timestamp, as numpy has no native date type
    Date(Vec<i64>),
    /// unix timestamp, as numpy has no native datetime type
    DateTime(Vec<i64>),
}

impl ColVec {
    pub fn type_name(&self) -> &'static str {
        match self {
            ColVec::U8(_) => "u8",
            ColVec::I8(_) => "i8",
            ColVec::U16(_) => "u16",
            ColVec::I16(_) => "i16",
            ColVec::U32(_) => "u32",
            ColVec::I32(_) => "i32",
            ColVec::U64(_) => "u64",
            ColVec::I64(_) => "i64",
            ColVec::F32(_) => "f32",
            ColVec::F64(_) => "f64",
            ColVec::Date(_) => "date",
            ColVec::DateTime(_) => "datetime",
        }
    }
}

pub async fn query_all(mut ch: ClientHandle, query_string: String) -> ChResult<HashMap<String, ColVec>> {
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
            },
            SqlType::DateTime(_) => {
                let u = column.iter::<DateTime<Tz>>()?
                    .map(|d| d.timestamp())
                    .collect();
                ColVec::DateTime(u)
            },
            _ => {
                error!("unsupported column type {} for column {}", column.sql_type().to_string(), column.name());
                return Err(ChError::Other(Cow::from("unsupported column type")));
            }
        };
        out_rows.insert(column.name().to_string(), values);
    }
    Ok(out_rows)
}

/// return all rows from the query and uncompact the h3index in the h3index column, all other columns get duplicated accordingly
pub async fn query_all_with_uncompacting(mut ch: ClientHandle, query_string: String, h3index_set: HashSet<u64>) -> ChResult<HashMap<String, ColVec>> {
    let h3_res = if let Some(first) = h3index_set.iter().next() {
        Index::from(*first).resolution()
    } else {
        return Err(ChError::Other(Cow::from("no h3indexes given")));
    };
    let block = ch.query(query_string).fetch_all().await?;

    let h3index_column = if let Some(c) = block.columns().iter()
        .find(|c| c.name() == "h3index") {
        c
    } else {
        return Err(ChError::Other(Cow::from("no h3index column found")));
    };

    // the number denoting how often a row must be duplicated
    // to match the number of uncompacted h3indexes
    let mut row_multiplicators = Vec::new();

    let h3_vec = {
        let mut h3_vec = Vec::new();
        for h3index in h3index_column.iter::<u64>()? {
            let idx = Index::from(*h3index);
            let m = if idx.resolution() < h3_res {
                let mut valid_children = idx.get_children(h3_res).drain(..)
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
                return Err(ChError::Other(Cow::from("too small resolution during uncompacting")));
            };
            row_multiplicators.push(m);
        }
        ColVec::U64(h3_vec)
    };

    let mut out_rows = HashMap::new();
    for column in block.columns() {
        if column.name() == "h3index" {
            continue
        }

        macro_rules! multiply_column {
            ($cvtype:ident, $itertype:ty, $conv:expr) => {
            {
                let mut values = Vec::new();
                let mut pos = 0_usize;
                for v in column.iter::<$itertype>()?.map($conv) {
                    for _i in 0..row_multiplicators[pos] {
                        values.push(v.clone())
                    }
                    pos += 1;
                };
                ColVec::$cvtype(values)
            }
            };
            ($cvtype:ident, $itertype:ty) => {
                 multiply_column!($cvtype, $itertype, |v| v)
            };
        }
        // TODO: how to handle nullable columns?
        // TODO: move column data without cloning
        let values = match column.sql_type() {
            SqlType::UInt8 => multiply_column!(U8, u8),
            SqlType::UInt16 => multiply_column!(U16, u16),
            SqlType::UInt32 => multiply_column!(U32, u32),
            SqlType::UInt64 => multiply_column!(U64, u64),
            SqlType::Int8 => multiply_column!(I8, i8),
            SqlType::Int16 => multiply_column!(I16, i16),
            SqlType::Int32 => multiply_column!(I32, i32),
            SqlType::Int64 => multiply_column!(I64, i64),
            SqlType::Float32 => multiply_column!(F32, f32),
            SqlType::Float64 => multiply_column!(F64, f64),
            SqlType::Date => multiply_column!(Date, Date<Tz>, |d| d.and_hms(12, 0, 0).timestamp()),
            SqlType::DateTime(_) => multiply_column!(DateTime, DateTime<Tz>, |d| d.timestamp()),
            _ => {
                error!("unsupported column type {} for column {}", column.sql_type().to_string(), column.name());
                return Err(ChError::Other(Cow::from("unsupported column type")));
            }
        };
        out_rows.insert(column.name().to_string(), values);
    }
    out_rows.insert("h3index".to_string(), h3_vec);
    Ok(out_rows)
}

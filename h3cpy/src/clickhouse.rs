use std::collections::HashMap;

use clickhouse_rs::{
    ClientHandle,
    errors::Error as ChError,
    errors::Result as ChResult,
    Pool,
};
use futures_util::StreamExt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::{PyResult, PyErr};
use tokio::runtime::Runtime;
use log::warn;

use h3cpy_int::compacted_tables::find_tablesets;
use crate::{
    inspect::TableSet as TableSetWrapper,
};


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


pub async fn query_all(mut ch: ClientHandle, query_string: String) -> ChResult<bool> {
    let block = ch.query(query_string).fetch_all().await?;

    for column in block.columns() {
        //column.name()
    }

    Ok(true)
}


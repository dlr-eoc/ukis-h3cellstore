use std::collections::{HashMap, HashSet};

use pyo3::exceptions::PyRuntimeError;
use pyo3::{PyErr, PyResult};
use tokio::runtime::Runtime;

use h3cpy_int::clickhouse::query::{
    list_tablesets, query_all, query_all_with_uncompacting, query_returns_rows,
};
use h3cpy_int::clickhouse_rs::{errors::Error as ChError, errors::Result as ChResult, Pool};
use h3cpy_int::compacted_tables::TableSet;
use h3cpy_int::ColVec;

fn ch_to_pyerr(ch_err: ChError) -> PyErr {
    PyRuntimeError::new_err(format!("clickhouse error: {:?}", ch_err))
}

fn ch_to_pyresult<T>(res: ChResult<T>) -> PyResult<T> {
    match res {
        Ok(v) => Ok(v),
        Err(e) => Err(ch_to_pyerr(e)),
    }
}

/// a synchronous api for the async clickhouse query functions of the _int crate
pub struct ClickhousePool {
    pool: Pool,
    rt: Runtime,
}

impl ClickhousePool {
    pub fn create(db_url: &str) -> PyResult<ClickhousePool> {
        let rt = match Runtime::new() {
            Ok(rt) => rt,
            Err(e) => {
                return Err(PyRuntimeError::new_err(format!(
                    "could not create tokio rt: {:?}",
                    e
                )));
            }
        };
        Ok(Self {
            pool: Pool::new(db_url),
            rt,
        })
    }

    pub fn query_all(&mut self, query_string: String) -> PyResult<HashMap<String, ColVec>> {
        let p = &self.pool;
        ch_to_pyresult(self.rt.block_on(async {
            let client = p.get_handle().await?;
            query_all(client, query_string).await
        }))
    }

    pub fn query_returns_rows(&mut self, query_string: String) -> PyResult<bool> {
        let p = &self.pool;
        ch_to_pyresult(self.rt.block_on(async {
            let client = p.get_handle().await?;
            query_returns_rows(client, query_string).await
        }))
    }

    pub fn query_all_with_uncompacting(
        &mut self,
        query_string: String,
        h3index_set: HashSet<u64>,
    ) -> PyResult<HashMap<String, ColVec>> {
        let p = &self.pool;
        ch_to_pyresult(self.rt.block_on(async {
            let client = p.get_handle().await?;
            query_all_with_uncompacting(client, query_string, h3index_set).await
        }))
    }

    pub fn list_tablesets(&mut self) -> PyResult<HashMap<String, TableSet>> {
        let p = &self.pool;
        ch_to_pyresult(self.rt.block_on(async {
            let client = p.get_handle().await?;
            list_tablesets(client).await
        }))
    }
}

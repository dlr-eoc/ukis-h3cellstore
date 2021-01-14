use pyo3::{PyErr, PyResult};
use pyo3::exceptions::PyRuntimeError;
use tokio::runtime::Runtime;

use h3cpy_int::{
    clickhouse_rs::{
        ClientHandle,
        errors::Error as ChError,
        errors::Result as ChResult,
        Pool,
    }
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

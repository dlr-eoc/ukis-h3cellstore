use numpy::{IntoPyArray, Ix1, PyArray};
use pyo3::{
    exceptions::{
        PyIndexError,
        PyValueError,
    },
    prelude::*,
    Python,
    wrap_pyfunction,
};

use h3cpy_int::{
    clickhouse::ColVec
};

use crate::{
    clickhouse::RuntimedPool,
    connection::{
        ClickhouseConnection,
        ResultSet,
    },
    inspect::{
        CompactedTable,
        TableSet,
    },
};

mod window;
mod inspect;
mod connection;
mod geometry;
mod clickhouse;


pub fn intresult_to_pyresult<T>(res: std::result::Result<T, h3cpy_int::error::Error>) -> PyResult<T> {
    match res {
        Ok(v) => Ok(v),
        Err(e) => Err(PyValueError::new_err(e.to_string()))
    }
}

/// version of the module
#[pyfunction]
fn version() -> PyResult<String> { Ok(env!("CARGO_PKG_VERSION").to_string()) }

/// open a connection to clickhouse
#[pyfunction]
fn create_connection(db_url: &str) -> PyResult<ClickhouseConnection> {
    Ok(ClickhouseConnection {
        rp: RuntimedPool::create(db_url)?
    })
}

macro_rules! resultset_drain_column_fn {
        ($fnname:ident, $dtype:ty, $cvtype:ident) => {
            #[pyfunction]
            fn $fnname(rs: &mut ResultSet, column_name: &str) -> PyResult<Py<PyArray<$dtype, Ix1>>> {
                if let Some(cv) = rs.column_data.get_mut(column_name) {
                    if let ColVec::$cvtype(v) = cv {
                        let data = std::mem::take(v);
                        let gil = Python::acquire_gil();
                        let py = gil.python();
                        Ok(data.into_pyarray(py).to_owned())
                    } else {
                        Err(PyValueError::new_err(format!("column {} is not accessible as type {}", column_name, stringify!($dtype))))
                    }
                } else {
                    Err(PyIndexError::new_err(format!("unknown column {}", column_name)))
                }
            }
        };
    }

resultset_drain_column_fn!(resultset_drain_column_u8, u8, U8);
resultset_drain_column_fn!(resultset_drain_column_i8, i8, I8);
resultset_drain_column_fn!(resultset_drain_column_u16, u16, U16);
resultset_drain_column_fn!(resultset_drain_column_i16, i16, I16);
resultset_drain_column_fn!(resultset_drain_column_u32, u32, U32);
resultset_drain_column_fn!(resultset_drain_column_i32, i32, I32);
resultset_drain_column_fn!(resultset_drain_column_u64, u64, U64);
resultset_drain_column_fn!(resultset_drain_column_i64, i64, I64);
resultset_drain_column_fn!(resultset_drain_column_f32, f32, F32);
resultset_drain_column_fn!(resultset_drain_column_f64, f64, F64);
resultset_drain_column_fn!(resultset_drain_column_date, i64, Date);
resultset_drain_column_fn!(resultset_drain_column_datetime, i64, DateTime);


/// A Python module implemented in Rust.
#[pymodule]
fn h3cpy(py: Python, m: &PyModule) -> PyResult<()> {
    env_logger::init();

    m.add("CompactedTable", py.get_type::<CompactedTable>())?;
    m.add("TableSet", py.get_type::<TableSet>())?;
    m.add("ClickhouseConnection", py.get_type::<ClickhouseConnection>())?;
    m.add("ResultSet", py.get_type::<ResultSet>())?;
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(create_connection, m)?)?;

    m.add_function(wrap_pyfunction!(resultset_drain_column_u8, m)?)?;
    m.add_function(wrap_pyfunction!(resultset_drain_column_i8, m)?)?;
    m.add_function(wrap_pyfunction!(resultset_drain_column_u16, m)?)?;
    m.add_function(wrap_pyfunction!(resultset_drain_column_i16, m)?)?;
    m.add_function(wrap_pyfunction!(resultset_drain_column_u32, m)?)?;
    m.add_function(wrap_pyfunction!(resultset_drain_column_i32, m)?)?;
    m.add_function(wrap_pyfunction!(resultset_drain_column_u64, m)?)?;
    m.add_function(wrap_pyfunction!(resultset_drain_column_i64, m)?)?;
    m.add_function(wrap_pyfunction!(resultset_drain_column_f32, m)?)?;
    m.add_function(wrap_pyfunction!(resultset_drain_column_f64, m)?)?;
    m.add_function(wrap_pyfunction!(resultset_drain_column_date, m)?)?;
    m.add_function(wrap_pyfunction!(resultset_drain_column_datetime, m)?)?;

    Ok(())
}
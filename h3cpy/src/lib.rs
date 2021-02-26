use numpy::{Ix1, PyArray, PyReadonlyArray1};
use pyo3::{
    exceptions::{
        PyIndexError,
        PyValueError,
    },
    prelude::*,
    Python,
    wrap_pyfunction,
};

use h3cpy_int::ColVec;

use crate::{
    clickhouse::{
        ClickhouseConnection,
        ResultSet,
    },
    inspect::{
        CompactedTable,
        TableSet,
    },
    syncapi::ClickhousePool,
};

mod window;
mod inspect;
mod clickhouse;
mod pywrap;
mod syncapi;

/// version of the module
#[pyfunction]
fn version() -> PyResult<String> { Ok(env!("CARGO_PKG_VERSION").to_string()) }

/// open a connection to clickhouse
#[pyfunction]
fn create_connection(db_url: &str) -> PyResult<ClickhouseConnection> {
    Ok(ClickhouseConnection::new(ClickhousePool::create(db_url)?))
}

macro_rules! resultset_drain_column_fn {
        ($fnname:ident, $dtype:ty, $cvtype:ident) => {
            #[pyfunction]
            fn $fnname(rs: &mut ResultSet, column_name: &str) -> PyResult<Py<PyArray<$dtype, Ix1>>> {
                if let Some(cv) = rs.column_data.get_mut(column_name) {
                    if let ColVec::$cvtype(v) = cv {
                        let data = std::mem::take(v);
                        Ok(crate::pywrap::vec_to_numpy_owned(data))
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


/// calculate the convex hull of an array og h3 indexes
#[pyfunction]
fn h3indexes_convex_hull(np_array: PyReadonlyArray1<u64>) -> PyResult<crate::pywrap::Polygon> {
    let view = np_array.as_array();
    Ok(h3cpy_int::algorithm::h3indexes_convex_hull(&view).into())
}

/// A Python module implemented in Rust.
#[pymodule]
fn h3cpy(py: Python, m: &PyModule) -> PyResult<()> {
    env_logger::init();

    m.add("CompactedTable", py.get_type::<CompactedTable>())?;
    m.add("TableSet", py.get_type::<TableSet>())?;
    m.add("ClickhouseConnection", py.get_type::<ClickhouseConnection>())?;
    m.add("ResultSet", py.get_type::<ResultSet>())?;
    m.add("Polygon", py.get_type::<crate::pywrap::Polygon>())?;
    m.add("H3IndexesContainedIn", py.get_type::<crate::pywrap::H3IndexesContainedIn>())?;

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

    m.add_function(wrap_pyfunction!(h3indexes_convex_hull, m)?)?;

    Ok(())
}
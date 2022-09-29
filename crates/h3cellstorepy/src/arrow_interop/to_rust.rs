use pyo3::exceptions::PyValueError;
use pyo3::ffi::Py_uintptr_t;
use pyo3::prelude::*;

use h3cellstore::export::polars::export::arrow::datatypes::DataType as ArrowDataType;
use h3cellstore::export::polars::export::arrow::ffi;
use h3cellstore::export::polars::export::rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, ParallelIterator,
};
use h3cellstore::export::polars::prelude::{ArrayRef, DataFrame, Series};
use polars_core::utils::accumulate_dataframes_vertical;
use polars_core::POOL;

use crate::error::{IntoPyResult, ToCustomPyErr};

/// from https://github.com/pola-rs/polars/blob/d1e5b1062c6872cd030b04b96505d2fac36b5376/py-polars/src/arrow_interop/to_rust.rs
pub fn array_to_rust(obj: &PyAny) -> PyResult<ArrayRef> {
    // prepare a pointer to receive the Array struct
    let array = Box::new(ffi::ArrowArray::empty());
    let schema = Box::new(ffi::ArrowSchema::empty());

    let array_ptr = &*array as *const ffi::ArrowArray;
    let schema_ptr = &*schema as *const ffi::ArrowSchema;

    // make the conversion through PyArrow's private API
    // this changes the pointer's memory and is thus unsafe. In particular, `_export_to_c` can go out of bounds
    obj.call_method1(
        "_export_to_c",
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;

    unsafe {
        let field = ffi::import_field_from_c(schema.as_ref()).into_pyresult()?;
        let array = ffi::import_array_from_c(*array, field.data_type).into_pyresult()?;
        Ok(array)
    }
}

pub fn to_rust_df(rb: &[&PyAny]) -> PyResult<DataFrame> {
    let schema = rb
        .get(0)
        .ok_or_else(|| PyValueError::new_err("empty table"))?
        .getattr("schema")?;
    let names = schema.getattr("names")?.extract::<Vec<String>>()?;

    let dfs = rb
        .iter()
        .map(|rb| {
            let mut run_parallel = false;

            let columns = (0..names.len())
                .map(|i| {
                    let array = rb.call_method1("column", (i,))?;
                    let arr = array_to_rust(array)?;
                    run_parallel |= matches!(
                        arr.data_type(),
                        ArrowDataType::Utf8 | ArrowDataType::Dictionary(_, _, _)
                    );
                    Ok(arr)
                })
                .collect::<PyResult<Vec<_>>>()?;

            // we parallelize this part because we can have dtypes that are not zero copy
            // for instance utf8 -> large-utf8
            // dict encoded to categorical
            let columns = if run_parallel {
                POOL.install(|| {
                    columns
                        .into_par_iter()
                        .enumerate()
                        .map(|(i, arr)| Series::try_from((names[i].as_str(), arr)).into_pyresult())
                        .collect::<PyResult<Vec<_>>>()
                })
            } else {
                columns
                    .into_iter()
                    .enumerate()
                    .map(|(i, arr)| Series::try_from((names[i].as_str(), arr)).into_pyresult())
                    .collect::<PyResult<Vec<_>>>()
            }?;

            DataFrame::new(columns).into_pyresult()
        })
        .collect::<PyResult<Vec<_>>>()?;

    accumulate_dataframes_vertical(dfs).map_err(|e| e.to_custom_pyerr())
}

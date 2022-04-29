use h3cellstore::export::arrow_h3::export::polars::frame::ArrowChunk;
use h3cellstore::export::arrow_h3::export::polars::prelude::ArrowField;
use h3cellstore::export::clickhouse_arrow_grpc::export::arrow2::array::ArrayRef;
use h3cellstore::export::clickhouse_arrow_grpc::export::arrow2::ffi;
use pyo3::ffi::Py_uintptr_t;
use pyo3::prelude::PyModule;
use pyo3::{PyObject, PyResult, Python, ToPyObject};

/// RecordBatch to Python.
///
/// From https://github.com/pola-rs/polars/blob/96980ff86d59d03f673862042d737a5bc601d3b2/py-polars/src/arrow_interop/to_py.rs#L37
pub(crate) fn to_py_rb(
    rb: &ArrowChunk,
    names: &[&str],
    py: Python,
    pyarrow: &PyModule,
) -> PyResult<PyObject> {
    let mut arrays = Vec::with_capacity(rb.len());

    for array in rb.columns() {
        let array_object = to_py_array(array.clone(), py, pyarrow)?;
        arrays.push(array_object);
    }

    let record = pyarrow
        .getattr("RecordBatch")?
        .call_method1("from_arrays", (arrays, names.to_vec()))?;

    Ok(record.to_object(py))
}

/// Arrow array to Python.
///  from https://github.com/pola-rs/polars/blob/96980ff86d59d03f673862042d737a5bc601d3b2/py-polars/src/arrow_interop/to_py.rs#L8
pub(crate) fn to_py_array(array: ArrayRef, py: Python, pyarrow: &PyModule) -> PyResult<PyObject> {
    let array_ptr = Box::new(ffi::ArrowArray::empty());
    let schema_ptr = Box::new(ffi::ArrowSchema::empty());

    let array_ptr = Box::into_raw(array_ptr);
    let schema_ptr = Box::into_raw(schema_ptr);

    unsafe {
        ffi::export_field_to_c(
            &ArrowField::new("", array.data_type().clone(), true),
            schema_ptr,
        );
        ffi::export_array_to_c(array, array_ptr);
    };

    let array = pyarrow.getattr("Array")?.call_method1(
        "_import_from_c",
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;

    unsafe {
        Box::from_raw(array_ptr);
        Box::from_raw(schema_ptr);
    };

    Ok(array.to_object(py))
}
use pyo3::ffi::Py_uintptr_t;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use pyo3::PyClass;

use h3cellstore::export::arrow_h3::export::polars::frame::{ArrowChunk, DataFrame};
use h3cellstore::export::arrow_h3::export::polars::prelude::ArrowField;
use h3cellstore::export::arrow_h3::H3DataFrame;
use h3cellstore::export::clickhouse_arrow_grpc::export::arrow2::array::ArrayRef;
use h3cellstore::export::clickhouse_arrow_grpc::export::arrow2::ffi;

#[pyclass]
pub struct PyH3DataFrame {
    h3df: H3DataFrame,
}

#[pymethods]
impl PyH3DataFrame {
    pub fn shape(&self) -> (usize, usize) {
        self.h3df.dataframe.shape()
    }

    pub fn h3index_column_name(&self) -> String {
        self.h3df.h3index_column_name.clone()
    }

    pub fn to_arrow(&mut self, py: Python) -> PyResult<Vec<PyObject>> {
        dataframe_to_arrow(py, &mut self.h3df.dataframe)
    }
}

impl From<H3DataFrame> for PyH3DataFrame {
    fn from(h3df: H3DataFrame) -> Self {
        Self { h3df }
    }
}

#[pyclass]
pub struct PyDataFrame {
    df: DataFrame,
}

#[pymethods]
impl PyDataFrame {
    pub fn shape(&self) -> (usize, usize) {
        self.df.shape()
    }

    pub fn to_arrow(&mut self, py: Python) -> PyResult<Vec<PyObject>> {
        dataframe_to_arrow(py, &mut self.df)
    }
}

impl From<DataFrame> for PyDataFrame {
    fn from(df: DataFrame) -> Self {
        Self { df }
    }
}

fn dataframe_to_arrow(py: Python, df: &mut DataFrame) -> PyResult<Vec<PyObject>> {
    // mostly from https://github.com/pola-rs/polars/blob/8b2db30ac18d219f4c3d02e2d501d2966cf58930/py-polars/src/dataframe.rs#L557
    df.rechunk();
    let pyarrow = py.import("pyarrow")?;
    let names = df.get_column_names();

    let rbs = df
        .iter_chunks()
        .map(|rb| to_py_rb(&rb, &names, py, pyarrow))
        .collect::<PyResult<_>>()?;
    Ok(rbs)
}

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

/// return wrapped in a python `DataFrameWrapper` instance
pub fn wrapped_frame<T: PyClass>(
    py: Python,
    frame: impl Into<PyClassInitializer<T>>,
) -> PyResult<PyObject> {
    let obj = PyCell::new(py, frame)?.to_object(py);

    let module = py.import("h3cellstorepy.frame")?;
    let args = PyTuple::new(py, [obj]);
    Ok(module
        .getattr("DataFrameWrapper")?
        .call1(args)?
        .to_object(py))
}

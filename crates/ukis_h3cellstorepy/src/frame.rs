use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};
use pyo3::PyClass;
use ukis_h3cellstore::export::h3ron::H3Cell;
use ukis_h3cellstore::export::h3ron_polars::frame::H3DataFrame;

use crate::arrow_interop::to_py::to_py_rb;
use crate::arrow_interop::to_rust::to_rust_df;
use ukis_h3cellstore::export::polars::frame::DataFrame;

/// A wrapper for internal dataframe with an associated name for the column containing H3 cells.
///
/// Allows exporting the data to arrow recordbatches using the `to_arrow` method.
///
/// This class should not be used directly in python, it is used within `DataFrameWrapper`.
#[pyclass]
pub struct PyH3DataFrame {
    h3df: H3DataFrame<H3Cell>,
}

#[pymethods]
impl PyH3DataFrame {
    pub fn shape(&self) -> (usize, usize) {
        self.h3df.dataframe().shape()
    }

    pub fn h3index_column_name(&self) -> String {
        self.h3df.h3index_column_name().to_string()
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn to_arrow(&mut self) -> PyResult<Vec<PyObject>> {
        dataframe_to_arrow(self.h3df.dataframe_mut())
    }
}

impl From<H3DataFrame<H3Cell>> for PyH3DataFrame {
    fn from(h3df: H3DataFrame<H3Cell>) -> Self {
        Self { h3df }
    }
}

/// A wrapper for internal dataframe.
///
/// Allows exporting the data to arrow recordbatches using the `to_arrow` method.
///
/// This class should not be used directly in python, it is used within `DataFrameWrapper`.
#[pyclass]
pub struct PyDataFrame {
    df: DataFrame,
}

#[pymethods]
impl PyDataFrame {
    pub fn shape(&self) -> (usize, usize) {
        self.df.shape()
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn to_arrow(&mut self) -> PyResult<Vec<PyObject>> {
        dataframe_to_arrow(&mut self.df)
    }
}

impl From<DataFrame> for PyDataFrame {
    fn from(df: DataFrame) -> Self {
        Self { df }
    }
}

fn dataframe_to_arrow(df: &mut DataFrame) -> PyResult<Vec<PyObject>> {
    // mostly from https://github.com/pola-rs/polars/blob/8b2db30ac18d219f4c3d02e2d501d2966cf58930/py-polars/src/dataframe.rs#L557
    df.align_chunks();

    Python::with_gil(|py| {
        let pyarrow = py.import("pyarrow")?;
        let names = df.get_column_names();

        let rbs = df
            .iter_chunks()
            .map(|rb| to_py_rb(&rb, &names, py, pyarrow))
            .collect::<PyResult<_>>()?;
        Ok(rbs)
    })
}

pub trait ToDataframeWrapper {
    /// return wrapped in a python `DataFrameWrapper` instance
    fn to_dataframewrapper(self) -> PyResult<PyObject>;
}

impl ToDataframeWrapper for PyH3DataFrame {
    fn to_dataframewrapper(self) -> PyResult<PyObject> {
        wrapped_frame(self)
    }
}

impl ToDataframeWrapper for PyDataFrame {
    fn to_dataframewrapper(self) -> PyResult<PyObject> {
        wrapped_frame(self)
    }
}

impl ToDataframeWrapper for H3DataFrame<H3Cell> {
    fn to_dataframewrapper(self) -> PyResult<PyObject> {
        PyH3DataFrame::from(self).to_dataframewrapper()
    }
}

impl ToDataframeWrapper for DataFrame {
    fn to_dataframewrapper(self) -> PyResult<PyObject> {
        PyDataFrame::from(self).to_dataframewrapper()
    }
}

fn frame_module(py: Python) -> PyResult<&PyModule> {
    py.import(concat!(env!("CARGO_PKG_NAME"), ".frame"))
}

/// return wrapped in a python `DataFrameWrapper` instance
fn wrapped_frame<T: PyClass>(frame: impl Into<PyClassInitializer<T>>) -> PyResult<PyObject> {
    Python::with_gil(|py| {
        let obj = PyCell::new(py, frame)?.to_object(py);
        let args = PyTuple::new(py, [obj]);
        Ok(frame_module(py)?
            .getattr("DataFrameWrapper")?
            .call1(args)?
            .to_object(py))
    })
}

pub fn dataframe_from_pyany(obj: &PyAny) -> PyResult<DataFrame> {
    Python::with_gil(|py| {
        let wrapped = frame_module(py)?
            .getattr("ensure_wrapped")?
            .call1(PyTuple::new(py, [obj]))?;

        let output = wrapped
            .getattr("to_arrow")?
            .call0()?
            .getattr("to_batches")? // provided by pyarrow.Table
            .call0()?;
        let mut arrow_chunks = vec![];
        if let Ok(tuple) = output.downcast::<PyTuple>() {
            arrow_chunks.extend(tuple.iter());
        } else if let Ok(list) = output.downcast::<PyList>() {
            arrow_chunks.extend(list.iter());
        } else {
            return Err(PyValueError::new_err(
                "received unsupported output from to_arrow method",
            ));
        }
        to_rust_df(&arrow_chunks)
    })
}

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};
use pyo3::PyClass;

use crate::arrow_interop::to_py::to_py_rb;
use crate::arrow_interop::to_rust::to_rust_df;
use h3cellstore::export::arrow_h3::export::polars::frame::DataFrame;
use h3cellstore::export::arrow_h3::H3DataFrame;

/// A wrapper for internal dataframe with an associated name for the column containing H3 cells.
///
/// Allows exporting the data to arrow recordbatches using the `to_arrow` method.
///
/// This class should not be used directly in python, it is used within `DataFrameWrapper`.
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

    #[allow(clippy::wrong_self_convention)]
    pub fn to_arrow(&mut self, py: Python) -> PyResult<Vec<PyObject>> {
        dataframe_to_arrow(py, &mut self.h3df.dataframe)
    }
}

impl From<H3DataFrame> for PyH3DataFrame {
    fn from(h3df: H3DataFrame) -> Self {
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

pub trait ToDataframeWrapper {
    /// return wrapped in a python `DataFrameWrapper` instance
    fn to_dataframewrapper(self, py: Python) -> PyResult<PyObject>;
}

impl ToDataframeWrapper for PyH3DataFrame {
    fn to_dataframewrapper(self, py: Python) -> PyResult<PyObject> {
        wrapped_frame(py, self)
    }
}

impl ToDataframeWrapper for PyDataFrame {
    fn to_dataframewrapper(self, py: Python) -> PyResult<PyObject> {
        wrapped_frame(py, self)
    }
}

impl ToDataframeWrapper for H3DataFrame {
    fn to_dataframewrapper(self, py: Python) -> PyResult<PyObject> {
        PyH3DataFrame::from(self).to_dataframewrapper(py)
    }
}

impl ToDataframeWrapper for DataFrame {
    fn to_dataframewrapper(self, py: Python) -> PyResult<PyObject> {
        PyDataFrame::from(self).to_dataframewrapper(py)
    }
}

/// return wrapped in a python `DataFrameWrapper` instance
fn wrapped_frame<T: PyClass>(
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

pub fn dataframe_from_pyany(py: Python, obj: &PyAny) -> PyResult<DataFrame> {
    let module = py.import("h3cellstorepy.frame")?;
    let wrapped = module
        .getattr("ensure_wrapped")?
        .call1(PyTuple::new(py, [obj]))?;
    let arrow_chunks = {
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
        arrow_chunks
    };
    to_rust_df(&arrow_chunks)
}

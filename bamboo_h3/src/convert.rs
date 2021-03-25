use bamboo_h3_int::fileio::{deserialize_from, serialize_into};
use bamboo_h3_int::ColVec;
use h3ron::Index;
use numpy::{IntoPyArray, Ix1, PyArray, PyReadonlyArray1};
use pyo3::exceptions::{PyIOError, PyIndexError, PyValueError};
use pyo3::prelude::*;
use pyo3::PyMappingProtocol;
use std::collections::HashMap;
use std::fs::File;

pub fn check_index_valid(index: &Index) -> PyResult<()> {
    if !index.is_valid() {
        Err(PyValueError::new_err(format!(
            "invalid h3index given: {}",
            index.h3index()
        )))
    } else {
        Ok(())
    }
}

pub fn intresult_to_pyresult<T>(
    res: std::result::Result<T, bamboo_h3_int::error::Error>,
) -> PyResult<T> {
    match res {
        Ok(v) => Ok(v),
        Err(e) => Err(PyValueError::new_err(e.to_string())),
    }
}

/// convert a Vec to a numpy array
pub fn vec_to_numpy_owned<T: numpy::Element>(in_vec: Vec<T>) -> Py<PyArray<T, Ix1>> {
    let gil = Python::acquire_gil();
    let py = gil.python();
    in_vec.into_pyarray(py).to_owned()
}

/// dataframe columns passed from python to this library
#[derive(FromPyObject)]
pub enum DataFrameColumnData<'a> {
    U8(PyReadonlyArray1<'a, u8>),
    U16(PyReadonlyArray1<'a, u16>),
    U32(PyReadonlyArray1<'a, u32>),
    U64(PyReadonlyArray1<'a, u64>),
    I8(PyReadonlyArray1<'a, i8>),
    I16(PyReadonlyArray1<'a, i16>),
    I32(PyReadonlyArray1<'a, i32>),
    I64(PyReadonlyArray1<'a, i64>),
    F32(PyReadonlyArray1<'a, f32>),
    F64(PyReadonlyArray1<'a, f64>),
    // TODO: Date and DateTime
}

impl Into<ColVec> for DataFrameColumnData<'_> {
    fn into(self) -> ColVec {
        match self {
            Self::U8(ra) => ColVec::U8(ra.as_array().to_vec()),
            Self::U16(ra) => ColVec::U16(ra.as_array().to_vec()),
            Self::U32(ra) => ColVec::U32(ra.as_array().to_vec()),
            Self::U64(ra) => ColVec::U64(ra.as_array().to_vec()),
            Self::I8(ra) => ColVec::I8(ra.as_array().to_vec()),
            Self::I16(ra) => ColVec::I16(ra.as_array().to_vec()),
            Self::I32(ra) => ColVec::I32(ra.as_array().to_vec()),
            Self::I64(ra) => ColVec::I64(ra.as_array().to_vec()),
            Self::F32(ra) => ColVec::F32(ra.as_array().to_vec()),
            Self::F64(ra) => ColVec::F64(ra.as_array().to_vec()),
            // TODO: Date and DateTime
        }
    }
}

#[pyclass]
pub struct ColumnSet {
    inner: bamboo_h3_int::ColumnSet,
}

#[pymethods]
impl ColumnSet {
    #[new]
    fn new() -> Self {
        Self {
            inner: Default::default(),
        }
    }

    fn add_numpy_column(&mut self, column_name: String, data: DataFrameColumnData) -> PyResult<()> {
        intresult_to_pyresult(self.inner.add_column(column_name, data.into()))
    }

    #[getter]
    /// get the names and types of the columns in the df
    fn get_column_types(&self) -> PyResult<HashMap<String, String>> {
        Ok(self.inner.column_type_names())
    }

    #[getter]
    fn get_empty(&self) -> PyResult<bool> {
        Ok(self.inner.is_empty())
    }

    fn write_to(&self, filename: String) -> PyResult<()> {
        let outfile =
            File::create(filename).map_err(|e| PyIOError::new_err(format!("io error: {:?}", e)))?;
        intresult_to_pyresult(serialize_into(outfile, &self.inner))?;
        Ok(())
    }

    #[staticmethod]
    fn read_from(filename: String) -> PyResult<Self> {
        let infile =
            File::open(filename).map_err(|e| PyIOError::new_err(format!("io error: {:?}", e)))?;
        let inner: bamboo_h3_int::ColumnSet = intresult_to_pyresult(deserialize_from(infile))?;
        Ok(Self { inner })
    }
}

// creating multiple impls is ugly - replace this in the future
macro_rules! columnset_drain_column_fn {
    ($fnname:ident, $dtype:ty, $cvtype:ident) => {
        #[pymethods]
        impl ColumnSet {
            fn $fnname(&mut self, column_name: &str) -> PyResult<Py<PyArray<$dtype, Ix1>>> {
                if let Some(cv) = self.inner.columns.get_mut(column_name) {
                    if let ColVec::$cvtype(v) = cv {
                        let data = std::mem::take(v);

                        // remove new completely as the type matches
                        self.inner.columns.remove(column_name);
                        if self.inner.columns.is_empty() {
                            self.inner.size = None;
                        }

                        Ok(crate::convert::vec_to_numpy_owned(data))
                    } else {
                        Err(PyValueError::new_err(format!(
                            "column {} is not accessible as type {}",
                            column_name,
                            stringify!($dtype)
                        )))
                    }
                } else {
                    Err(PyIndexError::new_err(format!(
                        "unknown column {}",
                        column_name
                    )))
                }
            }
        }
    };
}

columnset_drain_column_fn!(drain_column_u8, u8, U8);
columnset_drain_column_fn!(drain_column_i8, i8, I8);
columnset_drain_column_fn!(drain_column_u16, u16, U16);
columnset_drain_column_fn!(drain_column_i16, i16, I16);
columnset_drain_column_fn!(drain_column_u32, u32, U32);
columnset_drain_column_fn!(drain_column_i32, i32, I32);
columnset_drain_column_fn!(drain_column_u64, u64, U64);
columnset_drain_column_fn!(drain_column_i64, i64, I64);
columnset_drain_column_fn!(drain_column_f32, f32, F32);
columnset_drain_column_fn!(drain_column_f64, f64, F64);
columnset_drain_column_fn!(drain_column_date, i64, Date);
columnset_drain_column_fn!(drain_column_datetime, i64, DateTime);

#[pyproto]
impl PyMappingProtocol for ColumnSet {
    fn __len__(&self) -> usize {
        self.inner.len()
    }
}

impl From<HashMap<String, ColVec>> for ColumnSet {
    fn from(columns: HashMap<String, ColVec>) -> Self {
        Self {
            inner: columns.into(),
        }
    }
}

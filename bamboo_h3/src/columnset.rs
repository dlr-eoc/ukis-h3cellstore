use std::collections::HashMap;

use chrono::{Date, DateTime, TimeZone, Utc};
use chrono_tz::Tz;
use itertools::Itertools;
use numpy::{IntoPyArray, Ix1, PyArray, PyReadonlyArray1};
use pyo3::exceptions::{PyIndexError, PyValueError};
use pyo3::prelude::*;
use pyo3::{PyObjectProtocol, PySequenceProtocol};

use bamboo_h3_int::ColVec;

use crate::error::IntoPyResult;

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
    // Date and DateTime are handled via ColumnSet.add_numpy_date[time]_column
}

impl From<DataFrameColumnData<'_>> for ColVec {
    fn from(dfc: DataFrameColumnData<'_>) -> Self {
        match dfc {
            DataFrameColumnData::U8(ra) => ColVec::U8(ra.as_array().to_vec()),
            DataFrameColumnData::U16(ra) => ColVec::U16(ra.as_array().to_vec()),
            DataFrameColumnData::U32(ra) => ColVec::U32(ra.as_array().to_vec()),
            DataFrameColumnData::U64(ra) => ColVec::U64(ra.as_array().to_vec()),
            DataFrameColumnData::I8(ra) => ColVec::I8(ra.as_array().to_vec()),
            DataFrameColumnData::I16(ra) => ColVec::I16(ra.as_array().to_vec()),
            DataFrameColumnData::I32(ra) => ColVec::I32(ra.as_array().to_vec()),
            DataFrameColumnData::I64(ra) => ColVec::I64(ra.as_array().to_vec()),
            DataFrameColumnData::F32(ra) => ColVec::F32(ra.as_array().to_vec()),
            DataFrameColumnData::F64(ra) => ColVec::F64(ra.as_array().to_vec()),
            // Date and DateTime are handled via ColumnSet.add_numpy_date[time]_column
        }
    }
}

#[inline]
pub fn datetime_to_timestamp(dt: &DateTime<Tz>) -> i64 {
    dt.timestamp()
}

#[inline]
pub fn timestamp_to_datetime(timestamp: i64) -> DateTime<Tz> {
    Utc.timestamp(timestamp, 0).with_timezone(&Tz::UTC)
}

#[inline]
pub fn date_to_timestamp(d: &Date<Tz>) -> i64 {
    d.and_hms(0, 0, 0).timestamp()
}

#[inline]
pub fn timestamp_to_date(timestamp: i64) -> Date<Tz> {
    Utc.timestamp(timestamp, 0).with_timezone(&Tz::UTC).date()
}

#[pyclass]
pub struct ColumnSet {
    pub(crate) inner: bamboo_h3_int::ColumnSet,
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
        self.inner
            .add_column(column_name, data.into())
            .into_pyresult()
    }

    /// add a datetime column using UTC UNIX timestamps
    fn add_numpy_datetime_column(
        &mut self,
        column_name: String,
        data: PyReadonlyArray1<i64>,
    ) -> PyResult<()> {
        self.inner
            .add_column(
                column_name,
                ColVec::DateTime(
                    data.as_array()
                        .iter()
                        .map(|timestamp| timestamp_to_datetime(*timestamp))
                        .collect(),
                ),
            )
            .into_pyresult()
    }

    /// add a date column using UTC UNIX timestamps
    fn add_numpy_date_column(
        &mut self,
        column_name: String,
        data: PyReadonlyArray1<i64>,
    ) -> PyResult<()> {
        self.inner
            .add_column(
                column_name,
                ColVec::Date(
                    data.as_array()
                        .iter()
                        .map(|timestamp| timestamp_to_date(*timestamp))
                        .collect(),
                ),
            )
            .into_pyresult()
    }

    #[getter]
    /// get the names and types of the columns in the df
    fn get_column_types(&self) -> HashMap<String, String> {
        self.inner.column_type_names()
    }

    #[getter]
    fn get_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn to_compacted(&self, h3index_column_name: String) -> PyResult<Self> {
        Ok(Self {
            inner: self
                .inner
                .to_compacted(&h3index_column_name)
                .into_pyresult()?,
        })
    }

    #[args(validate_indexes = "true")]
    fn split_by_resolution(
        &self,
        h3index_column_name: String,
        validate_indexes: bool,
    ) -> PyResult<HashMap<u8, Self>> {
        let out = self
            .inner
            .split_by_resolution(&h3index_column_name, validate_indexes)
            .into_pyresult()?
            .drain()
            .map(|(h3res, cs)| (h3res, Self { inner: cs }))
            .collect();
        Ok(out)
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

                        // remove the column completely as the type matches
                        self.inner.columns.remove(column_name);
                        if self.inner.columns.is_empty() {
                            self.inner.size = None;
                        }

                        Ok(vec_to_numpy_owned(data))
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

macro_rules! columnset_drain_timestamp_column_fn {
    ($fnname:ident, $cvtype:ident, $conv_closure:expr) => {
        #[pymethods]
        impl ColumnSet {
            fn $fnname(&mut self, column_name: &str) -> PyResult<Py<PyArray<i64, Ix1>>> {
                if let Some(cv) = self.inner.columns.get_mut(column_name) {
                    if let ColVec::$cvtype(v) = cv {
                        let mut data = std::mem::take(v);

                        // remove the column completely as the type matches
                        self.inner.columns.remove(column_name);
                        if self.inner.columns.is_empty() {
                            self.inner.size = None;
                        }
                        let timestamps = data.drain(..).map($conv_closure).collect();
                        Ok(vec_to_numpy_owned(timestamps))
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
columnset_drain_timestamp_column_fn!(drain_column_date, Date, |d| date_to_timestamp(&d));
columnset_drain_timestamp_column_fn!(drain_column_datetime, DateTime, |d| datetime_to_timestamp(
    &d
));

#[pyproto]
impl PySequenceProtocol for ColumnSet {
    fn __len__(&self) -> usize {
        self.inner.len()
    }
}

#[pyproto]
impl PyObjectProtocol for ColumnSet {
    fn __repr__(&self) -> String {
        let keys = self.inner.columns.keys().sorted().join(", ");
        format!("ColumnSet({})[{} rows]", keys, self.inner.len())
    }

    fn __bool__(&self) -> bool {
        !self.inner.is_empty()
    }
}

impl From<HashMap<String, ColVec>> for ColumnSet {
    fn from(columns: HashMap<String, ColVec>) -> Self {
        Self {
            inner: columns.into(),
        }
    }
}

impl From<bamboo_h3_int::ColumnSet> for ColumnSet {
    fn from(cs: bamboo_h3_int::ColumnSet) -> Self {
        Self { inner: cs }
    }
}

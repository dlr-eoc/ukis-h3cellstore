use bamboo_h3_int::ColVec;
use numpy::PyReadonlyArray1;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::PyMappingProtocol;
use std::collections::HashMap;

/// dataframe columns passed from python to this library
#[derive(FromPyObject)]
pub enum DataFrameColumnData<'a> {
    U8(PyReadonlyArray1<'a, u8>),
    U16(PyReadonlyArray1<'a, u16>),
    U32(PyReadonlyArray1<'a, u32>),
    U64(PyReadonlyArray1<'a, u64>),
}

impl Into<ColVec> for DataFrameColumnData<'_> {
    fn into(self) -> ColVec {
        match self {
            Self::U8(ra) => ColVec::U8(ra.as_array().to_vec()),
            Self::U16(ra) => ColVec::U16(ra.as_array().to_vec()),
            Self::U32(ra) => ColVec::U32(ra.as_array().to_vec()),
            Self::U64(ra) => ColVec::U64(ra.as_array().to_vec()),
        }
    }
}

#[pyclass]
pub struct DataFrameContents {
    pub columns: HashMap<String, ColVec>,

    /// length of all of the columns in the dataframe
    size: Option<usize>,
}

impl DataFrameContents {
    /// create without validating the lenghts of the columns
    pub fn from_columns(columns: HashMap<String, ColVec>) -> Self {
        let size = columns
            .iter()
            .next()
            .map_or(None, |(_, colvec)| Some(colvec.len()));
        Self { columns, size }
    }

    pub fn add_column(&mut self, column_name: String, colvec: ColVec) -> PyResult<()> {
        // enforce all colvecs having the same length
        if let Some(size) = self.size {
            if colvec.len() != size {
                return Err(PyValueError::new_err(format!(
                    "column has the wrong length, expected: {}, found: {}",
                    size,
                    colvec.len()
                )));
            }
        } else {
            self.size = Some(colvec.len())
        }
        self.columns.insert(column_name, colvec);
        Ok(())
    }

    pub fn column_type_names(&self) -> PyResult<HashMap<String, String>> {
        Ok(self
            .columns
            .iter()
            .map(|(name, data)| (name.clone(), data.type_name().to_string()))
            .collect())
    }

    pub fn is_empty(&self) -> bool {
        self.size.is_none() || self.size == Some(0)
    }

    pub fn len(&self) -> usize {
        self.size.unwrap_or(0)
    }
}

impl Default for DataFrameContents {
    fn default() -> Self {
        Self {
            columns: Default::default(),
            size: None,
        }
    }
}

#[pymethods]
impl DataFrameContents {
    #[new]
    fn new() -> Self {
        Self {
            columns: Default::default(),
            size: None,
        }
    }

    fn add_numpy_column(&mut self, column_name: String, data: DataFrameColumnData) -> PyResult<()> {
        self.add_column(column_name, data.into())
    }
}

#[pyproto]
impl PyMappingProtocol for DataFrameContents {
    fn __len__(&self) -> usize {
        self.len()
    }
}

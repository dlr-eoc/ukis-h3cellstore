use bamboo_h3_int::ColVec;
use numpy::PyReadonlyArray1;
use pyo3::prelude::*;
use std::collections::HashMap;

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
}

#[pymethods]
impl DataFrameContents {

    #[staticmethod]
    pub fn create() -> Self {
        Self {
            columns: Default::default(),
        }
    }

    pub fn add_numpy_column(&mut self, column_name: String, data: DataFrameColumnData) {
        self.columns.insert(column_name, data.into());
    }
}

use crate::error::Error;
use std::collections::HashMap;
use serde::{Serialize, Deserialize};

/// a vector of column values
///
/// all enum variants ending with "N" are nullable
#[derive(Serialize, Deserialize, Debug)]
pub enum ColVec {
    U8(Vec<u8>),
    U8N(Vec<Option<u8>>),
    I8(Vec<i8>),
    I8N(Vec<Option<i8>>),
    U16(Vec<u16>),
    U16N(Vec<Option<u16>>),
    I16(Vec<i16>),
    I16N(Vec<Option<i16>>),
    U32(Vec<u32>),
    U32N(Vec<Option<u32>>),
    I32(Vec<i32>),
    I32N(Vec<Option<i32>>),
    U64(Vec<u64>),
    U64N(Vec<Option<u64>>),
    I64(Vec<i64>),
    I64N(Vec<Option<i64>>),
    F32(Vec<f32>),
    F32N(Vec<Option<f32>>),
    F64(Vec<f64>),
    F64N(Vec<Option<f64>>),
    /// unix timestamp, as numpy has no native date type
    Date(Vec<i64>),
    DateN(Vec<Option<i64>>),
    /// unix timestamp, as numpy has no native datetime type
    DateTime(Vec<i64>),
    DateTimeN(Vec<Option<i64>>),
}

impl ColVec {
    pub fn type_name(&self) -> &'static str {
        match self {
            ColVec::U8(_) => "u8",
            ColVec::U8N(_) => "u8n",
            ColVec::I8(_) => "i8",
            ColVec::I8N(_) => "i8n",
            ColVec::U16(_) => "u16",
            ColVec::U16N(_) => "u16n",
            ColVec::I16(_) => "i16",
            ColVec::I16N(_) => "i16n",
            ColVec::U32(_) => "u32",
            ColVec::U32N(_) => "u32n",
            ColVec::I32(_) => "i32",
            ColVec::I32N(_) => "i32n",
            ColVec::U64(_) => "u64",
            ColVec::U64N(_) => "u64n",
            ColVec::I64(_) => "i64",
            ColVec::I64N(_) => "i64n",
            ColVec::F32(_) => "f32",
            ColVec::F32N(_) => "f32n",
            ColVec::F64(_) => "f64",
            ColVec::F64N(_) => "f64n",
            ColVec::Date(_) => "date",
            ColVec::DateN(_) => "date_n",
            ColVec::DateTime(_) => "datetime",
            ColVec::DateTimeN(_) => "datetime_n",
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            ColVec::U8(v) => v.is_empty(),
            ColVec::U8N(v) => v.is_empty(),
            ColVec::I8(v) => v.is_empty(),
            ColVec::I8N(v) => v.is_empty(),
            ColVec::U16(v) => v.is_empty(),
            ColVec::U16N(v) => v.is_empty(),
            ColVec::I16(v) => v.is_empty(),
            ColVec::I16N(v) => v.is_empty(),
            ColVec::U32(v) => v.is_empty(),
            ColVec::U32N(v) => v.is_empty(),
            ColVec::I32(v) => v.is_empty(),
            ColVec::I32N(v) => v.is_empty(),
            ColVec::U64(v) => v.is_empty(),
            ColVec::U64N(v) => v.is_empty(),
            ColVec::I64(v) => v.is_empty(),
            ColVec::I64N(v) => v.is_empty(),
            ColVec::F32(v) => v.is_empty(),
            ColVec::F32N(v) => v.is_empty(),
            ColVec::F64(v) => v.is_empty(),
            ColVec::F64N(v) => v.is_empty(),
            ColVec::Date(v) => v.is_empty(),
            ColVec::DateN(v) => v.is_empty(),
            ColVec::DateTime(v) => v.is_empty(),
            ColVec::DateTimeN(v) => v.is_empty(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ColVec::U8(v) => v.len(),
            ColVec::U8N(v) => v.len(),
            ColVec::I8(v) => v.len(),
            ColVec::I8N(v) => v.len(),
            ColVec::U16(v) => v.len(),
            ColVec::U16N(v) => v.len(),
            ColVec::I16(v) => v.len(),
            ColVec::I16N(v) => v.len(),
            ColVec::U32(v) => v.len(),
            ColVec::U32N(v) => v.len(),
            ColVec::I32(v) => v.len(),
            ColVec::I32N(v) => v.len(),
            ColVec::U64(v) => v.len(),
            ColVec::U64N(v) => v.len(),
            ColVec::I64(v) => v.len(),
            ColVec::I64N(v) => v.len(),
            ColVec::F32(v) => v.len(),
            ColVec::F32N(v) => v.len(),
            ColVec::F64(v) => v.len(),
            ColVec::F64N(v) => v.len(),
            ColVec::Date(v) => v.len(),
            ColVec::DateN(v) => v.len(),
            ColVec::DateTime(v) => v.len(),
            ColVec::DateTimeN(v) => v.len(),
        }
    }
}

///
/// a set of columns with their values
///
/// This can be seen as the equivalent to the pandas DateFrame but limited
/// to storage only. Additionally, this would be the point where arrow support
/// could be added (using arrows StructArray)
#[derive(Serialize, Deserialize, Debug)]
pub struct ColumnSet {
    pub columns: HashMap<String, ColVec>,

    /// length of all of the columns in the dataframe
    pub size: Option<usize>,
}

impl ColumnSet {
    /// create without validating the lenghts of the columns
    pub fn from_columns(columns: HashMap<String, ColVec>) -> Self {
        let size = columns
            .iter()
            .next()
            .map_or(None, |(_, colvec)| Some(colvec.len()));
        Self { columns, size }
    }

    pub fn add_column(&mut self, column_name: String, colvec: ColVec) -> Result<(), Error> {
        // enforce all colvecs having the same length
        if let Some(size) = self.size {
            if colvec.len() != size {
                return Err(Error::DifferentColumnLength(
                    column_name,
                    colvec.len(),
                    size,
                ));
            }
        } else {
            self.size = Some(colvec.len())
        }
        self.columns.insert(column_name, colvec);
        Ok(())
    }

    pub fn column_type_names(&self) -> HashMap<String, String> {
        self.columns
            .iter()
            .map(|(name, data)| (name.clone(), data.type_name().to_string()))
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.size.is_none() || self.size == Some(0)
    }

    pub fn len(&self) -> usize {
        self.size.unwrap_or(0)
    }
}

impl Default for ColumnSet {
    fn default() -> Self {
        Self {
            columns: Default::default(),
            size: None,
        }
    }
}

impl From<HashMap<String, ColVec>> for ColumnSet {
    fn from(columns: HashMap<String, ColVec>) -> Self {
        Self::from_columns(columns)
    }
}

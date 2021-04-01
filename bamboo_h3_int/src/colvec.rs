use std::collections::HashMap;

use chrono::{Date, DateTime};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};

use crate::common::Named;
use crate::error::Error;
use std::iter::FromIterator;

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Datatype {
    U8,
    U8N,
    I8,
    I8N,
    U16,
    U16N,
    I16,
    I16N,
    U32,
    U32N,
    I32,
    I32N,
    U64,
    U64N,
    I64,
    I64N,
    F32,
    F32N,
    F64,
    F64N,
    Date,
    DateN,
    DateTime,
    DateTimeN,
}

impl Datatype {
    pub fn from_str(value: &str) -> Result<Self, Error> {
        Ok(match value.to_lowercase().as_str() {
            "u8" | "uint8" => Datatype::U8,
            "u8n" => Datatype::U8N,
            "i8" | "int8" => Datatype::I8,
            "i8n" => Datatype::I8N,
            "u16" | "uint16" => Datatype::U16,
            "u16n" => Datatype::U16N,
            "i16" | "int16" => Datatype::I16,
            "i16n" => Datatype::I16N,
            "u32" | "uint32" => Datatype::U32,
            "u32n" => Datatype::U32N,
            "i32" | "int32" => Datatype::I32,
            "i32n" => Datatype::I32N,
            "u64" | "uint64" => Datatype::U64,
            "u64n" => Datatype::U64N,
            "i64" | "int64" => Datatype::I64,
            "i64n" => Datatype::I64N,
            "f32" | "float32" => Datatype::F32,
            "f32n" => Datatype::F32N,
            "f64" | "float64" => Datatype::F64,
            "f64n" => Datatype::F64N,
            "date" => Datatype::Date,
            "date_n" => Datatype::DateN,
            "datetime" => Datatype::DateTime,
            "datetime_n" => Datatype::DateTimeN,
            _ => return Err(Error::UnknownDatatype(value.to_string())),
        })
    }

    pub fn is_nullable(&self) -> bool {
        // always list all variants of the enum to have the benefit of the compiler errors
        // when missing something
        match self {
            Datatype::U8
            | Datatype::I8
            | Datatype::U16
            | Datatype::I16
            | Datatype::U32
            | Datatype::I32
            | Datatype::U64
            | Datatype::I64
            | Datatype::F32
            | Datatype::F64
            | Datatype::Date
            | Datatype::DateTime => false,

            Datatype::U8N
            | Datatype::I8N
            | Datatype::U16N
            | Datatype::I16N
            | Datatype::U32N
            | Datatype::I32N
            | Datatype::U64N
            | Datatype::I64N
            | Datatype::F32N
            | Datatype::F64N
            | Datatype::DateN
            | Datatype::DateTimeN => true,
        }
    }

    pub fn is_temporal(&self) -> bool {
        matches!(
            self,
            Datatype::Date | Datatype::DateTime | Datatype::DateN | Datatype::DateTimeN
        )
    }
}

impl Named for Datatype {
    fn name(&self) -> &'static str {
        match self {
            Datatype::U8 => "u8",
            Datatype::U8N => "u8n",
            Datatype::I8 => "i8",
            Datatype::I8N => "i8n",
            Datatype::U16 => "u16",
            Datatype::U16N => "u16n",
            Datatype::I16 => "i16",
            Datatype::I16N => "i16n",
            Datatype::U32 => "u32",
            Datatype::U32N => "u32n",
            Datatype::I32 => "i32",
            Datatype::I32N => "i32n",
            Datatype::U64 => "u64",
            Datatype::U64N => "u64n",
            Datatype::I64 => "i64",
            Datatype::I64N => "i64n",
            Datatype::F32 => "f32",
            Datatype::F32N => "f32n",
            Datatype::F64 => "f64",
            Datatype::F64N => "f64n",
            Datatype::Date => "date",
            Datatype::DateN => "date_n",
            Datatype::DateTime => "datetime",
            Datatype::DateTimeN => "datetime_n",
        }
    }
}

impl std::fmt::Display for Datatype {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

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
    pub fn datatype(&self) -> Datatype {
        match self {
            ColVec::U8(_) => Datatype::U8,
            ColVec::U8N(_) => Datatype::U8N,
            ColVec::I8(_) => Datatype::I8,
            ColVec::I8N(_) => Datatype::I8N,
            ColVec::U16(_) => Datatype::U16,
            ColVec::U16N(_) => Datatype::U16N,
            ColVec::I16(_) => Datatype::I16,
            ColVec::I16N(_) => Datatype::I16N,
            ColVec::U32(_) => Datatype::U32,
            ColVec::U32N(_) => Datatype::U32N,
            ColVec::I32(_) => Datatype::I32,
            ColVec::I32N(_) => Datatype::I32N,
            ColVec::U64(_) => Datatype::U64,
            ColVec::U64N(_) => Datatype::U64N,
            ColVec::I64(_) => Datatype::I64,
            ColVec::I64N(_) => Datatype::I64N,
            ColVec::F32(_) => Datatype::F32,
            ColVec::F32N(_) => Datatype::F32N,
            ColVec::F64(_) => Datatype::F64,
            ColVec::F64N(_) => Datatype::F64N,
            ColVec::Date(_) => Datatype::Date,
            ColVec::DateN(_) => Datatype::DateN,
            ColVec::DateTime(_) => Datatype::DateTime,
            ColVec::DateTimeN(_) => Datatype::DateTimeN,
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

#[inline]
fn datetime_to_timestamp(dt: &DateTime<Tz>) -> i64 {
    dt.timestamp()
}

#[inline]
fn date_to_timestamp(d: &Date<Tz>) -> i64 {
    d.and_hms(12, 0, 0).timestamp()
}

macro_rules! vec_to_colvec_from_impl {
    ($vt:ty, $cvtype:ident) => {
        impl From<Vec<$vt>> for ColVec {
            fn from(v: Vec<$vt>) -> Self {
                ColVec::$cvtype(v)
            }
        }
    };
    ($vt:ty, $cvtype:ident, $converter_closure:expr) => {
        impl From<Vec<$vt>> for ColVec {
            fn from(mut v: Vec<$vt>) -> Self {
                ColVec::$cvtype(v.drain(..).map($converter_closure).collect())
            }
        }
    };
}

vec_to_colvec_from_impl!(u8, U8);
vec_to_colvec_from_impl!(i8, I8);
vec_to_colvec_from_impl!(u16, U16);
vec_to_colvec_from_impl!(i16, I16);
vec_to_colvec_from_impl!(u32, U32);
vec_to_colvec_from_impl!(i32, I32);
vec_to_colvec_from_impl!(u64, U64);
vec_to_colvec_from_impl!(i64, I64);
vec_to_colvec_from_impl!(f32, F32);
vec_to_colvec_from_impl!(f64, F64);
vec_to_colvec_from_impl!(Option<u8>, U8N);
vec_to_colvec_from_impl!(Option<i8>, I8N);
vec_to_colvec_from_impl!(Option<u16>, U16N);
vec_to_colvec_from_impl!(Option<i16>, I16N);
vec_to_colvec_from_impl!(Option<u32>, U32N);
vec_to_colvec_from_impl!(Option<i32>, I32N);
vec_to_colvec_from_impl!(Option<u64>, U64N);
vec_to_colvec_from_impl!(Option<i64>, I64N);
vec_to_colvec_from_impl!(Option<f32>, F32N);
vec_to_colvec_from_impl!(Option<f64>, F64N);
vec_to_colvec_from_impl!(Date<Tz>, Date, |d| date_to_timestamp(&d));
vec_to_colvec_from_impl!(Option<Date<Tz>>, DateN, |d| d
    .map(|inner| date_to_timestamp(&inner)));
vec_to_colvec_from_impl!(DateTime<Tz>, DateTime, |d| datetime_to_timestamp(&d));
vec_to_colvec_from_impl!(Option<DateTime<Tz>>, DateTimeN, |d| d
    .map(|inner| datetime_to_timestamp(&inner)));

macro_rules! iter_to_colvec_fromiterator_impl {
    ($vt:ty, $cvtype:ident) => {
        impl FromIterator<$vt> for ColVec {
            fn from_iter<T: IntoIterator<Item = $vt>>(iter: T) -> Self {
                ColVec::$cvtype(iter.into_iter().collect())
            }
        }
    };
    ($vt:ty, $cvtype:ident, $converter_closure:expr) => {
        impl FromIterator<$vt> for ColVec {
            fn from_iter<T: IntoIterator<Item = $vt>>(iter: T) -> Self {
                ColVec::$cvtype(iter.into_iter().map($converter_closure).collect())
            }
        }
    };
}

iter_to_colvec_fromiterator_impl!(u8, U8);
iter_to_colvec_fromiterator_impl!(i8, I8);
iter_to_colvec_fromiterator_impl!(u16, U16);
iter_to_colvec_fromiterator_impl!(i16, I16);
iter_to_colvec_fromiterator_impl!(u32, U32);
iter_to_colvec_fromiterator_impl!(i32, I32);
iter_to_colvec_fromiterator_impl!(u64, U64);
iter_to_colvec_fromiterator_impl!(i64, I64);
iter_to_colvec_fromiterator_impl!(f32, F32);
iter_to_colvec_fromiterator_impl!(f64, F64);
iter_to_colvec_fromiterator_impl!(Option<u8>, U8N);
iter_to_colvec_fromiterator_impl!(Option<i8>, I8N);
iter_to_colvec_fromiterator_impl!(Option<u16>, U16N);
iter_to_colvec_fromiterator_impl!(Option<i16>, I16N);
iter_to_colvec_fromiterator_impl!(Option<u32>, U32N);
iter_to_colvec_fromiterator_impl!(Option<i32>, I32N);
iter_to_colvec_fromiterator_impl!(Option<u64>, U64N);
iter_to_colvec_fromiterator_impl!(Option<i64>, I64N);
iter_to_colvec_fromiterator_impl!(Option<f32>, F32N);
iter_to_colvec_fromiterator_impl!(Option<f64>, F64N);
iter_to_colvec_fromiterator_impl!(Date<Tz>, Date, |d| date_to_timestamp(&d));
iter_to_colvec_fromiterator_impl!(Option<Date<Tz>>, DateN, |d| d
    .map(|inner| date_to_timestamp(&inner)));
iter_to_colvec_fromiterator_impl!(DateTime<Tz>, DateTime, |d| datetime_to_timestamp(&d));
iter_to_colvec_fromiterator_impl!(Option<DateTime<Tz>>, DateTimeN, |d| d
    .map(|inner| datetime_to_timestamp(&inner)));

///
/// a set of columns with their values
///
/// This can be seen as the equivalent to the pandas DateFrame but limited
/// to storage only. Additionally, this would be the point where arrow support
/// could be added (using arrows RecordBatch https://docs.rs/arrow/2.0.0/arrow/record_batch/struct.RecordBatch.html)
#[derive(Serialize, Deserialize, Debug)]
pub struct ColumnSet {
    pub columns: HashMap<String, ColVec>,

    /// length of all of the columns in the dataframe
    pub size: Option<usize>,
}

impl ColumnSet {
    /// create without validating the lenghts of the columns
    pub fn from_columns(columns: HashMap<String, ColVec>) -> Self {
        let size = columns.iter().next().map(|(_, colvec)| colvec.len());
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
            .map(|(name, data)| (name.clone(), data.datatype().name().to_string()))
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

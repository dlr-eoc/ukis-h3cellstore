use std::collections::HashMap;
use std::iter::FromIterator;

use chrono::{Date, DateTime};
use chrono_tz::Tz;
use itertools::repeat_n;
use ordered_float::OrderedFloat;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::common::Named;
use crate::error::Error;

const DT_DATE_NAME: &str = "date";
const DT_DATEN_NAME: &str = "date_n";
const DT_DATETIME_NAME: &str = "datetime";
const DT_DATETIMEN_NAME: &str = "datetime_n";
const DT_F32_NAME: &str = "f32";
const DT_F32N_NAME: &str = "f32n";
const DT_F64_NAME: &str = "f64";
const DT_F64N_NAME: &str = "f64n";
const DT_I16_NAME: &str = "i16";
const DT_I16N_NAME: &str = "i16n";
const DT_I32_NAME: &str = "i32";
const DT_I32N_NAME: &str = "i32n";
const DT_I64_NAME: &str = "i64";
const DT_I64N_NAME: &str = "i64n";
const DT_I8_NAME: &str = "i8";
const DT_I8N_NAME: &str = "i8n";
const DT_U16_NAME: &str = "u16";
const DT_U16N_NAME: &str = "u16n";
const DT_U32_NAME: &str = "u32";
const DT_U32N_NAME: &str = "u32n";
const DT_U64_NAME: &str = "u64";
const DT_U64N_NAME: &str = "u64n";
const DT_U8_NAME: &str = "u8";
const DT_U8N_NAME: &str = "u8n";

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
    pub fn from_name_str(value: &str) -> Result<Self, Error> {
        Ok(match value.to_lowercase().as_str() {
            DT_U8_NAME | "uint8" => Datatype::U8,
            DT_U8N_NAME => Datatype::U8N,
            DT_I8_NAME | "int8" => Datatype::I8,
            DT_I8N_NAME => Datatype::I8N,
            DT_U16_NAME | "uint16" => Datatype::U16,
            DT_U16N_NAME => Datatype::U16N,
            DT_I16_NAME | "int16" => Datatype::I16,
            DT_I16N_NAME => Datatype::I16N,
            DT_U32_NAME | "uint32" => Datatype::U32,
            DT_U32N_NAME => Datatype::U32N,
            DT_I32_NAME | "int32" => Datatype::I32,
            DT_I32N_NAME => Datatype::I32N,
            DT_U64_NAME | "uint64" => Datatype::U64,
            DT_U64N_NAME => Datatype::U64N,
            DT_I64_NAME | "int64" => Datatype::I64,
            DT_I64N_NAME => Datatype::I64N,
            DT_F32_NAME | "float32" => Datatype::F32,
            DT_F32N_NAME => Datatype::F32N,
            DT_F64_NAME | "float64" => Datatype::F64,
            DT_F64N_NAME => Datatype::F64N,
            DT_DATE_NAME => Datatype::Date,
            DT_DATEN_NAME => Datatype::DateN,
            DT_DATETIME_NAME => Datatype::DateTime,
            DT_DATETIMEN_NAME => Datatype::DateTimeN,
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
            Datatype::U8 => DT_U8_NAME,
            Datatype::U8N => DT_U8N_NAME,
            Datatype::I8 => DT_I8_NAME,
            Datatype::I8N => DT_I8N_NAME,
            Datatype::U16 => DT_U16_NAME,
            Datatype::U16N => DT_U16N_NAME,
            Datatype::I16 => DT_I16_NAME,
            Datatype::I16N => DT_I16N_NAME,
            Datatype::U32 => DT_U32_NAME,
            Datatype::U32N => DT_U32N_NAME,
            Datatype::I32 => DT_I32_NAME,
            Datatype::I32N => DT_I32N_NAME,
            Datatype::U64 => DT_U64_NAME,
            Datatype::U64N => DT_U64N_NAME,
            Datatype::I64 => DT_U64_NAME,
            Datatype::I64N => DT_I64N_NAME,
            Datatype::F32 => DT_F32_NAME,
            Datatype::F32N => DT_F32N_NAME,
            Datatype::F64 => DT_F64_NAME,
            Datatype::F64N => DT_F64N_NAME,
            Datatype::Date => DT_DATE_NAME,
            Datatype::DateN => DT_DATEN_NAME,
            Datatype::DateTime => DT_DATETIME_NAME,
            Datatype::DateTimeN => DT_DATETIMEN_NAME,
        }
    }
}

impl std::fmt::Display for Datatype {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
enum ColVecValue {
    U8(u8),
    U8N(Option<u8>),
    I8(i8),
    I8N(Option<i8>),
    U16(u16),
    U16N(Option<u16>),
    I16(i16),
    I16N(Option<i16>),
    U32(u32),
    U32N(Option<u32>),
    I32(i32),
    I32N(Option<i32>),
    U64(u64),
    U64N(Option<u64>),
    I64(i64),
    I64N(Option<i64>),
    F32(OrderedFloat<f32>),
    F32N(Option<OrderedFloat<f32>>),
    F64(OrderedFloat<f64>),
    F64N(Option<OrderedFloat<f64>>),
    /// unix timestamp, as numpy has no native date type
    Date(i64),
    DateN(Option<i64>),
    /// unix timestamp, as numpy has no native datetime type
    DateTime(i64),
    DateTimeN(Option<i64>),
}

// to allow using rayons parallel iterators with that type
unsafe impl Send for ColVecValue {}

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

    fn value_at(&self, index: usize) -> Option<ColVecValue> {
        match self {
            ColVec::U8(v) => v.get(index).map(|v| ColVecValue::U8(*v)),
            ColVec::U8N(v) => v.get(index).map(|v| ColVecValue::U8N(*v)),
            ColVec::I8(v) => v.get(index).map(|v| ColVecValue::I8(*v)),
            ColVec::I8N(v) => v.get(index).map(|v| ColVecValue::I8N(*v)),
            ColVec::U16(v) => v.get(index).map(|v| ColVecValue::U16(*v)),
            ColVec::U16N(v) => v.get(index).map(|v| ColVecValue::U16N(*v)),
            ColVec::I16(v) => v.get(index).map(|v| ColVecValue::I16(*v)),
            ColVec::I16N(v) => v.get(index).map(|v| ColVecValue::I16N(*v)),
            ColVec::U32(v) => v.get(index).map(|v| ColVecValue::U32(*v)),
            ColVec::U32N(v) => v.get(index).map(|v| ColVecValue::U32N(*v)),
            ColVec::I32(v) => v.get(index).map(|v| ColVecValue::I32(*v)),
            ColVec::I32N(v) => v.get(index).map(|v| ColVecValue::I32N(*v)),
            ColVec::U64(v) => v.get(index).map(|v| ColVecValue::U64(*v)),
            ColVec::U64N(v) => v.get(index).map(|v| ColVecValue::U64N(*v)),
            ColVec::I64(v) => v.get(index).map(|v| ColVecValue::I64(*v)),
            ColVec::I64N(v) => v.get(index).map(|v| ColVecValue::I64N(*v)),
            ColVec::F32(v) => v
                .get(index)
                .map(|v| ColVecValue::F32(OrderedFloat::from(*v))),
            ColVec::F32N(v) => v
                .get(index)
                .map(|v| ColVecValue::F32N(v.map(OrderedFloat::from))),
            ColVec::F64(v) => v
                .get(index)
                .map(|v| ColVecValue::F64(OrderedFloat::from(*v))),
            ColVec::F64N(v) => v
                .get(index)
                .map(|v| ColVecValue::F64N(v.map(OrderedFloat::from))),
            ColVec::Date(v) => v.get(index).map(|v| ColVecValue::Date(*v)),
            ColVec::DateN(v) => v.get(index).map(|v| ColVecValue::DateN(*v)),
            ColVec::DateTime(v) => v.get(index).map(|v| ColVecValue::DateTime(*v)),
            ColVec::DateTimeN(v) => v.get(index).map(|v| ColVecValue::DateTimeN(*v)),
        }
    }

    pub(crate) fn append(&mut self, other: &mut ColVec) -> Result<(), Error> {
        match (self, other) {
            (ColVec::U8(v), ColVec::U8(other_v)) => v.append(other_v),
            (ColVec::U8N(v), ColVec::U8N(other_v)) => v.append(other_v),
            (ColVec::I8(v), ColVec::I8(other_v)) => v.append(other_v),
            (ColVec::I8N(v), ColVec::I8N(other_v)) => v.append(other_v),
            (ColVec::U16(v), ColVec::U16(other_v)) => v.append(other_v),
            (ColVec::U16N(v), ColVec::U16N(other_v)) => v.append(other_v),
            (ColVec::I16(v), ColVec::I16(other_v)) => v.append(other_v),
            (ColVec::I16N(v), ColVec::I16N(other_v)) => v.append(other_v),
            (ColVec::U32(v), ColVec::U32(other_v)) => v.append(other_v),
            (ColVec::U32N(v), ColVec::U32N(other_v)) => v.append(other_v),
            (ColVec::I32(v), ColVec::I32(other_v)) => v.append(other_v),
            (ColVec::I32N(v), ColVec::I32N(other_v)) => v.append(other_v),
            (ColVec::U64(v), ColVec::U64(other_v)) => v.append(other_v),
            (ColVec::U64N(v), ColVec::U64N(other_v)) => v.append(other_v),
            (ColVec::I64(v), ColVec::I64(other_v)) => v.append(other_v),
            (ColVec::I64N(v), ColVec::I64N(other_v)) => v.append(other_v),
            (ColVec::F32(v), ColVec::F32(other_v)) => v.append(other_v),
            (ColVec::F32N(v), ColVec::F32N(other_v)) => v.append(other_v),
            (ColVec::F64(v), ColVec::F64(other_v)) => v.append(other_v),
            (ColVec::F64N(v), ColVec::F64N(other_v)) => v.append(other_v),
            (ColVec::Date(v), ColVec::Date(other_v)) => v.append(other_v),
            (ColVec::DateN(v), ColVec::DateN(other_v)) => v.append(other_v),
            (ColVec::DateTime(v), ColVec::DateTime(other_v)) => v.append(other_v),
            (ColVec::DateTimeN(v), ColVec::DateTimeN(other_v)) => v.append(other_v),
            (this_cv, other_cv) => {
                log::error!(
                    "colvecs of datatype {} can not be appended to colvecs of datatype {}",
                    other_cv.datatype().to_string(),
                    this_cv.datatype().to_string()
                );
                return Err(Error::IncompatibleDatatype);
            }
        }
        Ok(())
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
        // enforce all colvecs to have the same length
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

    pub fn to_compacted<T>(&self, h3index_column: &T) -> Result<Self, Error>
    where
        T: ToString,
    {
        let (h3index_column_name, h3index_vec) = {
            let hc = h3index_column.to_string();
            match self.columns.get(&hc) {
                None => return Err(Error::ColumnNotFound(hc)),
                Some(colvec) => match colvec {
                    ColVec::U64(colvec) => (hc, colvec),
                    _ => return Err(Error::InvalidColumn(hc)),
                },
            }
        };
        let other_columns: Vec<_> = self
            .columns
            .keys()
            .cloned()
            .filter(|cn| cn.as_str() != h3index_column_name)
            .collect();

        if other_columns.is_empty() {
            // single column Columset, so taking a shortcut by just compacting the single vec
            let mut hm = HashMap::new();
            hm.insert(
                h3index_column_name,
                ColVec::U64(h3ron::compact(h3index_vec)),
            );
            Ok(hm.into())
        } else {
            let size = match self.size {
                Some(s) => s,
                None => return Err(Error::EmptyIndexes), // TODO: better error
            };

            // create groups using the same values in the non-h3index columns
            let mut groups: HashMap<Vec<ColVecValue>, Vec<u64>> = HashMap::new();
            for i in 0..size {
                let mut this_group = Vec::with_capacity(other_columns.len());
                for c in other_columns.iter() {
                    this_group.push(
                        self.columns
                            .get(c)
                            .expect("missing column")
                            .value_at(i)
                            .expect("column vec too short"),
                    )
                }
                let h3index = h3index_vec.get(i).cloned().expect("h3index_vec too short");
                groups
                    .entry(this_group)
                    .and_modify(|v| v.push(h3index))
                    .or_insert_with(|| vec![h3index]);
            }

            let parallelize = self.len() > 100000 && groups.len() > 1;

            let mut outmap: HashMap<String, ColVec> = HashMap::new();
            for (mut group, h3indexes_compacted) in compact_groups(groups, parallelize) {
                // repeat each column value according to the number of h3indexes in the group
                for (col_index, col_value) in group.drain(..).enumerate() {
                    let mut col_cv = colvecvalue_to_colvec(col_value, h3indexes_compacted.len());
                    match outmap.get_mut(&other_columns[col_index]) {
                        Some(outmap_cv) => {
                            outmap_cv.append(&mut col_cv)?;
                        }
                        None => {
                            outmap.insert(other_columns[col_index].clone(), col_cv);
                        }
                    }
                }

                // add the h3indexes
                let mut h3index_colvec = ColVec::U64(h3indexes_compacted);
                match outmap.get_mut(&h3index_column_name) {
                    Some(outmap_cv) => {
                        outmap_cv.append(&mut h3index_colvec)?;
                    }
                    None => {
                        outmap.insert(h3index_column_name.clone(), h3index_colvec);
                    }
                }
            }
            Ok(outmap.into())
        }
    }
}

/// compact the give h3 indexes
///
/// prepares for compacting by removing all eventual duplicates as
/// this is nothing upstream `compact` can handle
fn compact(mut h3indexes: Vec<u64>) -> Vec<u64> {
    h3indexes.sort_unstable();
    h3indexes.dedup();
    h3ron::compact(&h3indexes)
}

fn compact_groups(
    mut groups: HashMap<Vec<ColVecValue>, Vec<u64>>,
    parallelize: bool,
) -> HashMap<Vec<ColVecValue>, Vec<u64>> {
    if parallelize {
        groups.par_drain().map(|(k, v)| (k, compact(v))).collect()
    } else {
        groups.drain().map(|(k, v)| (k, compact(v))).collect()
    }
}

fn colvecvalue_to_colvec(cvv: ColVecValue, repetitions: usize) -> ColVec {
    match cvv {
        ColVecValue::U8(v) => ColVec::U8(repeat_n(v, repetitions).collect()),
        ColVecValue::U8N(v) => ColVec::U8N(repeat_n(v, repetitions).collect()),
        ColVecValue::I8(v) => ColVec::I8(repeat_n(v, repetitions).collect()),
        ColVecValue::I8N(v) => ColVec::I8N(repeat_n(v, repetitions).collect()),
        ColVecValue::U16(v) => ColVec::U16(repeat_n(v, repetitions).collect()),
        ColVecValue::U16N(v) => ColVec::U16N(repeat_n(v, repetitions).collect()),
        ColVecValue::I16(v) => ColVec::I16(repeat_n(v, repetitions).collect()),
        ColVecValue::I16N(v) => ColVec::I16N(repeat_n(v, repetitions).collect()),
        ColVecValue::U32(v) => ColVec::U32(repeat_n(v, repetitions).collect()),
        ColVecValue::U32N(v) => ColVec::U32N(repeat_n(v, repetitions).collect()),
        ColVecValue::I32(v) => ColVec::I32(repeat_n(v, repetitions).collect()),
        ColVecValue::I32N(v) => ColVec::I32N(repeat_n(v, repetitions).collect()),
        ColVecValue::U64(v) => ColVec::U64(repeat_n(v, repetitions).collect()),
        ColVecValue::U64N(v) => ColVec::U64N(repeat_n(v, repetitions).collect()),
        ColVecValue::I64(v) => ColVec::I64(repeat_n(v, repetitions).collect()),
        ColVecValue::I64N(v) => ColVec::I64N(repeat_n(v, repetitions).collect()),
        ColVecValue::F32(v) => ColVec::F32(repeat_n(*v, repetitions).collect()),
        ColVecValue::F32N(v) => ColVec::F32N(repeat_n(v.map(|v| *v), repetitions).collect()),
        ColVecValue::F64(v) => ColVec::F64(repeat_n(*v, repetitions).collect()),
        ColVecValue::F64N(v) => ColVec::F64N(repeat_n(v.map(|v| *v), repetitions).collect()),
        ColVecValue::Date(v) => ColVec::Date(repeat_n(v, repetitions).collect()),
        ColVecValue::DateN(v) => ColVec::DateN(repeat_n(v, repetitions).collect()),
        ColVecValue::DateTime(v) => ColVec::DateTime(repeat_n(v, repetitions).collect()),
        ColVecValue::DateTimeN(v) => ColVec::DateTimeN(repeat_n(v, repetitions).collect()),
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

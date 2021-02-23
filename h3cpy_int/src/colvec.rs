/// a vector of column values
///
/// all enum variants ending with "N" are nullable
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
            ColVec::U8(_) | ColVec::U8N(_) => "u8",
            ColVec::I8(_) | ColVec::I8N(_) => "i8",
            ColVec::U16(_) | ColVec::U16N(_) => "u16",
            ColVec::I16(_) | ColVec::I16N(_) => "i16",
            ColVec::U32(_) | ColVec::U32N(_) => "u32",
            ColVec::I32(_) | ColVec::I32N(_) => "i32",
            ColVec::U64(_) | ColVec::U64N(_) => "u64",
            ColVec::I64(_) | ColVec::I64N(_)=> "i64",
            ColVec::F32(_) | ColVec::F32N(_) => "f32",
            ColVec::F64(_) | ColVec::F64N(_) => "f64",
            ColVec::Date(_) | ColVec::DateN(_) => "date",
            ColVec::DateTime(_) | ColVec::DateTimeN(_) => "datetime",
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
}

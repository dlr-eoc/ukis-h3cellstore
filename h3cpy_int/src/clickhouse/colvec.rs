pub enum ColVec {
    U8(Vec<u8>),
    I8(Vec<i8>),
    U16(Vec<u16>),
    I16(Vec<i16>),
    U32(Vec<u32>),
    I32(Vec<i32>),
    U64(Vec<u64>),
    I64(Vec<i64>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    /// unix timestamp, as numpy has no native date type
    Date(Vec<i64>),
    /// unix timestamp, as numpy has no native datetime type
    DateTime(Vec<i64>),
}

impl ColVec {
    pub fn type_name(&self) -> &'static str {
        match self {
            ColVec::U8(_) => "u8",
            ColVec::I8(_) => "i8",
            ColVec::U16(_) => "u16",
            ColVec::I16(_) => "i16",
            ColVec::U32(_) => "u32",
            ColVec::I32(_) => "i32",
            ColVec::U64(_) => "u64",
            ColVec::I64(_) => "i64",
            ColVec::F32(_) => "f32",
            ColVec::F64(_) => "f64",
            ColVec::Date(_) => "date",
            ColVec::DateTime(_) => "datetime",
        }
    }
}

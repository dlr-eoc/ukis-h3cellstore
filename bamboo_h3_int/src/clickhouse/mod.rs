use crate::{ColVec, ColumnSet};
use clickhouse_rs::Block;

pub mod compacted_tables;
pub mod query;
pub mod schema;
pub mod window;

impl From<ColumnSet> for Block {
    fn from(mut cs: ColumnSet) -> Self {
        let mut block = Self::with_capacity(cs.len());
        for (column_name, colvec) in cs.columns.drain() {
            block = match colvec {
                ColVec::U8(v) => block.column(&column_name, v),
                ColVec::U8N(v) => block.column(&column_name, v),
                ColVec::I8(v) => block.column(&column_name, v),
                ColVec::I8N(v) => block.column(&column_name, v),
                ColVec::U16(v) => block.column(&column_name, v),
                ColVec::U16N(v) => block.column(&column_name, v),
                ColVec::I16(v) => block.column(&column_name, v),
                ColVec::I16N(v) => block.column(&column_name, v),
                ColVec::U32(v) => block.column(&column_name, v),
                ColVec::U32N(v) => block.column(&column_name, v),
                ColVec::I32(v) => block.column(&column_name, v),
                ColVec::I32N(v) => block.column(&column_name, v),
                ColVec::U64(v) => block.column(&column_name, v),
                ColVec::U64N(v) => block.column(&column_name, v),
                ColVec::I64(v) => block.column(&column_name, v),
                ColVec::I64N(v) => block.column(&column_name, v),
                ColVec::F32(v) => block.column(&column_name, v),
                ColVec::F32N(v) => block.column(&column_name, v),
                ColVec::F64(v) => block.column(&column_name, v),
                ColVec::F64N(v) => block.column(&column_name, v),
                ColVec::Date(v) => block.column(&column_name, v),
                ColVec::DateN(v) => block.column(&column_name, v),
                ColVec::DateTime(v) => block.column(&column_name, v),
                ColVec::DateTimeN(v) => block.column(&column_name, v),
            };
        }
        block
    }
}

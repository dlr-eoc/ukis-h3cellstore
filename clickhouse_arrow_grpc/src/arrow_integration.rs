//!
//! implements the converting as described in [clickhouse io-formats](https://clickhouse.com/docs/en/interfaces/formats/#data_types-matching-arrow)
//! and casting to polars supported types
//!

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::compute::cast::cast;
use arrow2::datatypes::{DataType, Schema};
use arrow2::io::ipc::read::{read_file_metadata, FileReader};
use arrow2::io::ipc::write::FileWriter;
use polars_core::prelude::DataFrame;
use polars_core::utils::accumulate_dataframes_vertical;

use crate::Error;

enum ClickhouseArrowCast {
    None,
    Simple(DataType),
}

impl ClickhouseArrowCast {
    fn apply(&self, array: &Arc<dyn Array>) -> Result<Arc<dyn Array>, Error> {
        match self {
            Self::None => Ok(array.clone()),
            Self::Simple(dtype) => Ok(cast(array.as_ref(), dtype, Default::default())?.into()),
        }
    }
}

fn apply_casts_to_chunk(
    chunk: &Chunk<Arc<dyn Array>>,
    casts_to_perform: &[ClickhouseArrowCast],
) -> Result<Chunk<Arc<dyn Array>>, Error> {
    if chunk.arrays().len() != casts_to_perform.len() {
        return Err(Error::CastArrayLengthMismatch);
    }

    let mut casted = Vec::with_capacity(chunk.arrays().len());
    for (array, cast_to_perform) in chunk.arrays().iter().zip(casts_to_perform.iter()) {
        casted.push(cast_to_perform.apply(array)?);
    }

    Ok(Chunk::new(casted))
}

impl TryInto<DataFrame> for super::api::Result {
    type Error = Error;

    fn try_into(self) -> Result<DataFrame, Self::Error> {
        let mut cur = Cursor::new(self.output);
        let metadata = read_file_metadata(&mut cur)?;

        let mut fields = Vec::with_capacity(metadata.schema.fields.len());
        let mut casts_to_perform = Vec::with_capacity(metadata.schema.fields.len());
        let schema_fields_by_name: HashMap<_, _> = metadata
            .schema
            .fields
            .iter()
            .map(|field| (&field.name, field))
            .collect();

        dbg!(&self.output_columns);
        for output_column in self.output_columns.iter() {
            let schema_field = schema_fields_by_name
                .get(&output_column.name)
                .ok_or_else(|| Error::ArrowChunkMissingField(output_column.name.clone()))?;
            let (new_field, cast_to_perform) = match output_column.r#type.as_str() {
                "String" | "FixedString" => {
                    let mut new_field = (*schema_field).clone();
                    new_field.data_type = DataType::LargeUtf8;
                    let cast_to_perform = ClickhouseArrowCast::Simple(new_field.data_type.clone());
                    (new_field, cast_to_perform)
                }
                "Bool" => {
                    let mut new_field = (*schema_field).clone();
                    new_field.data_type = DataType::Boolean;
                    let cast_to_perform = ClickhouseArrowCast::Simple(new_field.data_type.clone());
                    (new_field, cast_to_perform)
                }
                _ => ((*schema_field).clone(), ClickhouseArrowCast::None),
            };
            fields.push(new_field);
            casts_to_perform.push(cast_to_perform);
        }

        let chunks = FileReader::new(cur, metadata, None).collect::<Result<Vec<_>, _>>()?;
        let mut dfs = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            dfs.push(DataFrame::try_from((
                apply_casts_to_chunk(&chunk, &casts_to_perform)?,
                fields.as_slice(),
            ))?);
        }

        let mut df = accumulate_dataframes_vertical(dfs)?;
        df.rechunk();
        Ok(df)
    }
}

pub fn serialize_for_clickhouse(df: &mut DataFrame) -> Result<Vec<u8>, Error> {
    let schema = df.schema().to_arrow();
    let mut new_fields = Vec::with_capacity(schema.fields.len());
    let mut casts_to_perform = Vec::with_capacity(schema.fields.len());
    for field in schema.fields.iter() {
        let (new_field, cast_to_perform) = match field.data_type {
            DataType::LargeUtf8 => {
                let mut new_field = field.clone();
                new_field.data_type = DataType::Utf8;
                let cast_to_perform = ClickhouseArrowCast::Simple(new_field.data_type.clone());
                (new_field, cast_to_perform)
            }
            _ => (field.clone(), ClickhouseArrowCast::None),
        };
        new_fields.push(new_field);
        casts_to_perform.push(cast_to_perform);
    }

    let new_schema = Schema::from(new_fields);
    let mut out_buf = vec![];

    let mut ipc_writer = FileWriter::try_new(&mut out_buf, &new_schema, None, Default::default())?;
    df.rechunk();

    for chunk in df.iter_chunks() {
        let new_chunk = apply_casts_to_chunk(&chunk, &casts_to_perform)?;
        ipc_writer.write(&new_chunk, None)?
    }
    ipc_writer.finish()?;
    Ok(out_buf)
}

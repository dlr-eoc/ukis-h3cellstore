//!
//! implements the converting as described in [clickhouse io-formats](https://clickhouse.com/docs/en/interfaces/formats/#data_types-matching-arrow)
//! and casting to polars supported types
//!

use std::collections::HashMap;
use std::io::Cursor;
use std::ops::Add;

use arrow2::array::{new_empty_array, Array, PrimitiveArray};
use arrow2::chunk::Chunk;
use arrow2::compute::arity::unary;
use arrow2::compute::cast::cast;
use arrow2::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow2::io::ipc::read::{read_file_metadata, FileReader};
use arrow2::io::ipc::write::FileWriter;
use polars_core::prelude::DataFrame;
use polars_core::series::Series;
use polars_core::utils::accumulate_dataframes_vertical;
use tracing::log::debug;

use crate::Error;

enum ClickhouseArrowCast {
    Simple(DataType),
    DateTimeFromChDate,
    DateTimeFromChDateTime,
}

impl ClickhouseArrowCast {
    #[allow(clippy::borrowed_box)]
    fn apply(&self, array: &Box<dyn Array>) -> Result<Box<dyn Array>, Error> {
        match self {
            Self::Simple(dtype) => Ok(cast(array.as_ref(), dtype, Default::default())?),
            Self::DateTimeFromChDate => {
                // A date. Stored in two bytes as the number of days since 1970-01-01 (unsigned).
                // Allows storing values from just after the beginning of the Unix Epoch to the upper
                // threshold defined by a constant at the compilation stage (currently, this is until
                // the year 2149, but the final fully-supported year is 2148).
                // Supported range of values: [1970-01-01, 2149-06-06].
                // Source: https://clickhouse.com/docs/en/sql-reference/data-types/date/
                #[allow(clippy::useless_conversion)]
                Ok(unary(
                    array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<u16>>()
                        .expect("Ch date expected to be u16"),
                    i32::from,
                    DataType::Date32,
                )
                .to_boxed()
                .into())
            }
            Self::DateTimeFromChDateTime => {
                // Allows to store an instant in time, that can be expressed as a calendar date and a time of a day.
                //
                // Syntax: DateTime([timezone])
                //
                // Supported range of values: [1970-01-01 00:00:00, 2106-02-07 06:28:15].
                // Resolution: 1 second.
                // Source: https://clickhouse.com/docs/en/sql-reference/data-types/datetime/
                // TODO: support timezones
                #[allow(clippy::useless_conversion)]
                Ok(unary(
                    array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<u32>>()
                        .expect("Ch datetime expected to be u32"),
                    |v| {
                        chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                            .expect("out of range date")
                            .and_hms_opt(0, 0, 0)
                            .expect("out of range datetime")
                            .add(chrono::Duration::seconds(v as i64))
                            .timestamp()
                    },
                    DataType::Timestamp(TimeUnit::Second, None),
                )
                .to_boxed()
                .into())
            }
        }
    }

    fn ouptut_datatype(&self) -> &DataType {
        match self {
            ClickhouseArrowCast::Simple(dt) => dt,
            ClickhouseArrowCast::DateTimeFromChDate => &DataType::Date32,
            ClickhouseArrowCast::DateTimeFromChDateTime => {
                &DataType::Timestamp(TimeUnit::Second, None)
            }
        }
    }
}

fn apply_casts_to_chunk(
    chunk: &Chunk<Box<dyn Array>>,
    casts_to_perform: &[Option<ClickhouseArrowCast>],
) -> Result<Chunk<Box<dyn Array>>, Error> {
    if chunk.arrays().len() != casts_to_perform.len() {
        return Err(Error::CastArrayLengthMismatch);
    }

    let mut casted = Vec::with_capacity(chunk.arrays().len());
    for (array, cast_to_perform) in chunk.arrays().iter().zip(casts_to_perform.iter()) {
        match cast_to_perform {
            Some(cast_to_perform) => casted.push(cast_to_perform.apply(array)?),
            None => casted.push(array.clone()),
        }
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

        // cast based on the output_column type info provided by clickhouse. In case this
        // is not set, this implementation should not fail and just return the dataframe without the
        // additional casting.
        for output_column in self.output_columns.iter() {
            let schema_field = schema_fields_by_name
                .get(&output_column.name)
                .ok_or_else(|| Error::ArrowChunkMissingField(output_column.name.clone()))?;
            //dbg!(
            //    &schema_field.name,
            //    &schema_field.data_type,
            //    output_column.r#type.as_str()
            //);
            let (new_field, cast_to_perform) =
                match (output_column.r#type.as_str(), &schema_field.data_type) {
                    ("String", DataType::Binary) | ("FixedString", DataType::Binary) => {
                        simple_cast(schema_field, DataType::LargeUtf8)
                    }
                    ("Bool", DataType::UInt8) => simple_cast(schema_field, DataType::Boolean),
                    ("Date", DataType::UInt16) => {
                        let mut new_field = (*schema_field).clone();
                        let cast_to_perform = ClickhouseArrowCast::DateTimeFromChDate;
                        new_field.data_type = cast_to_perform.ouptut_datatype().clone();
                        (new_field, Some(cast_to_perform))
                    }
                    ("DateTime", DataType::UInt32) => {
                        let mut new_field = (*schema_field).clone();
                        let cast_to_perform = ClickhouseArrowCast::DateTimeFromChDateTime;
                        new_field.data_type = cast_to_perform.ouptut_datatype().clone();
                        (new_field, Some(cast_to_perform))
                    }
                    _ => ((*schema_field).clone(), None),
                };
            fields.push(new_field);
            casts_to_perform.push(cast_to_perform);
        }

        let chunks = FileReader::new(cur, metadata, None, None).collect::<Result<Vec<_>, _>>()?;
        let mut dfs = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            dfs.push(DataFrame::try_from((
                apply_casts_to_chunk(&chunk, &casts_to_perform)?,
                fields.as_slice(),
            ))?);
        }

        if dfs.is_empty() {
            // See https://github.com/pola-rs/polars/blob/8b2db30ac18d219f4c3d02e2d501d2966cf58930/polars/polars-io/src/lib.rs#L127
            // Create an empty dataframe with the correct data types
            let empty_cols = fields
                .iter()
                .map(|fld| {
                    Series::try_from((fld.name.as_str(), new_empty_array(fld.data_type.clone())))
                })
                .collect::<polars_core::error::PolarsResult<_>>()?;
            Ok(DataFrame::new(empty_cols)?)
        } else {
            let mut df = accumulate_dataframes_vertical(dfs)?;
            df.rechunk();
            Ok(df)
        }
    }
}

fn simple_cast(schema_field: &Field, data_type: DataType) -> (Field, Option<ClickhouseArrowCast>) {
    debug!(
        "Casting field {} from {:?} to {:?}",
        &schema_field.name, &schema_field.data_type, &data_type
    );
    let mut new_field = (*schema_field).clone();
    new_field.data_type = data_type;
    let cast_to_perform = Some(ClickhouseArrowCast::Simple(new_field.data_type.clone()));
    (new_field, cast_to_perform)
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
                (new_field, Some(cast_to_perform))
            }
            _ => (field.clone(), None),
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

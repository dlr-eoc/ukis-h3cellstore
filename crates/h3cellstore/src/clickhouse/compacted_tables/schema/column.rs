use std::any::type_name;

#[cfg(feature = "use_serde")]
use serde::{Deserialize, Serialize};

use crate::clickhouse::compacted_tables::schema::{
    AggregationMethod, ClickhouseDataType, CompressionMethod, ValidateSchema,
};
use crate::{Error, Named};

#[derive(Debug, PartialEq, Eq, Clone)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub enum ColumnDefinition {
    /// a simple column which just stores data.
    /// The data will not get modified when the values get aggregated to coarser resolutions.
    Simple(SimpleColumn),

    /// a column storing an h3index
    /// h3 indexes will always be brought the resolution of the coarser table when generating parent
    /// resolutions
    H3Index,

    /// data stored in this column will be aggregated using the specified aggregation
    /// method when the coarser resolutions are generated
    ///
    /// Aggregation only happens **within** the batch written to
    /// the tables.
    WithAggregation(SimpleColumn, AggregationMethod),
}

impl ColumnDefinition {
    pub fn datatype(&self) -> ClickhouseDataType {
        match self {
            Self::H3Index => ClickhouseDataType::UInt64,
            Self::Simple(sc) => sc.datatype,
            Self::WithAggregation(sc, _) => sc.datatype,
        }
    }

    /// position in the sorting key (`ORDER BY`) in MergeTree tables
    /// which can be unterstood as a form of a primary key. Please consult
    /// https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/mergetree/
    /// for more
    pub fn order_key_position(&self) -> Option<u8> {
        match self {
            Self::H3Index => Some(0),
            Self::Simple(sc) => sc.order_key_position,
            Self::WithAggregation(sc, _) => sc.order_key_position,
        }
    }

    pub fn compression_method(&self) -> Option<&CompressionMethod> {
        match self {
            ColumnDefinition::Simple(sc) => sc.compression_method.as_ref(),
            ColumnDefinition::H3Index => None,
            ColumnDefinition::WithAggregation(sc, _) => sc.compression_method.as_ref(),
        }
    }
}

impl ValidateSchema for ColumnDefinition {
    fn validate(&self) -> Result<(), Error> {
        if let Self::WithAggregation(simple_column, aggregation_method) = self {
            if !(aggregation_method.is_applicable_to_datatype(&simple_column.datatype)) {
                return Err(Error::SchemaValidationError(
                    type_name::<Self>(),
                    format!(
                        "aggregation {} can not be applied to datatype {}",
                        aggregation_method.name(),
                        simple_column.datatype.name()
                    ),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub struct SimpleColumn {
    datatype: ClickhouseDataType,
    /// position in the sorting key (`ORDER BY`) in MergeTree tables
    /// which can be unterstood as a form of a primary key. Please consult
    /// https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/mergetree/
    /// for more
    order_key_position: Option<u8>,

    compression_method: Option<CompressionMethod>,
}

impl SimpleColumn {
    pub fn new(
        datatype: ClickhouseDataType,
        order_key_position: Option<u8>,
        compression_method: Option<CompressionMethod>,
    ) -> Self {
        Self {
            datatype,
            order_key_position,
            compression_method,
        }
    }
}

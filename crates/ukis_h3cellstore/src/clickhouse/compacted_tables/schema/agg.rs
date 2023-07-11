#[cfg(feature = "use_serde")]
use serde::{Deserialize, Serialize};

use crate::clickhouse::compacted_tables::schema::ClickhouseDataType;
use crate::Named;

/// Aggregations are only applied during aggregation, not compaction
#[derive(Debug, PartialEq, Eq, Clone)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub enum AggregationMethod {
    RelativeToCellArea,
    Sum,
    Max,
    Min,
    Average,
    /// set to Null in case different values are to be aggregated. Useful for categorical values
    SetNullOnConflict,
    // TODO: aggregation method to generate parent resolution for other h3index column
}

impl AggregationMethod {
    pub fn is_applicable_to_datatype(&self, datatype: &ClickhouseDataType, nullable: bool) -> bool {
        match self {
            Self::RelativeToCellArea => !datatype.is_temporal() && datatype.is_number(),
            Self::Sum => !datatype.is_temporal() && datatype.is_number(),
            Self::Max => datatype.is_number(),
            Self::Min => datatype.is_number(),
            Self::Average => datatype.is_number(),
            Self::SetNullOnConflict => nullable,
        }
    }

    pub fn disables_compaction(&self) -> bool {
        if matches!(self, Self::Sum) {
            // using sum disables compaction as reading previously compacted-stored will lead
            // to other values.
            // TODO: find a better solution for this
            true
        } else {
            false
        }
    }
}

impl Named for AggregationMethod {
    fn name(&self) -> &'static str {
        match self {
            Self::RelativeToCellArea => "relativetocellarea",
            Self::Max => "max",
            Self::Min => "min",
            Self::Sum => "sum",
            Self::Average => "average",
            Self::SetNullOnConflict => "setnullonconflict",
        }
    }
}

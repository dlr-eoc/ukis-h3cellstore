#[cfg(feature = "use_serde")]
use serde::{Deserialize, Serialize};

use crate::clickhouse::compacted_tables::schema::ClickhouseDataType;
use crate::Named;

#[derive(Debug, PartialEq, Clone)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub enum AggregationMethod {
    RelativeToCellArea,
    Sum,
    Max,
    Min,
    Average,
    // TODO: aggregation method to generate parent resolution for other h3index column
}

impl AggregationMethod {
    pub fn is_applicable_to_datatype(&self, datatype: &ClickhouseDataType) -> bool {
        match self {
            Self::RelativeToCellArea => !datatype.is_temporal() && datatype.is_number(),
            Self::Sum => !datatype.is_temporal() && datatype.is_number(),
            Self::Max => datatype.is_number(),
            Self::Min => datatype.is_number(),
            Self::Average => datatype.is_number(),
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
        }
    }
}

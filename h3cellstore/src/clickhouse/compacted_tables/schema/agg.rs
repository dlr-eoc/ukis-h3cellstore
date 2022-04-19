use crate::clickhouse::compacted_tables::schema::ClickhouseDataType;
use crate::Named;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
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
            Self::RelativeToCellArea => !datatype.is_temporal(),
            Self::Sum => !datatype.is_temporal(),
            Self::Max => true,
            Self::Min => true,
            Self::Average => true,
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

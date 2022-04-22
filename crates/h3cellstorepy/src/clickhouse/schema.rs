use lazy_static::lazy_static;
use std::num::ParseIntError;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use regex::Regex;

use h3cellstore::clickhouse::compacted_tables::schema::{
    AggregationMethod, ClickhouseDataType, ColumnDefinition, CompressionMethod, SimpleColumn,
    TableEngine, TemporalPartitioning, TemporalResolution, ValidateSchema,
};

use crate::error::IntoPyResult;

#[pyclass]
pub struct CompactedTableSchema {
    pub inner: h3cellstore::clickhouse::compacted_tables::schema::CompactedTableSchema,
}

#[pymethods]
impl CompactedTableSchema {
    fn validate(&self) -> PyResult<()> {
        self.inner.validate().into_pyresult()
    }

    fn to_json_string(&self) -> PyResult<String> {
        serde_json::to_string(&self.inner).into_pyresult()
    }

    #[staticmethod]
    fn from_json_string(instr: String) -> PyResult<Self> {
        Ok(Self {
            inner: serde_json::from_str(instr.as_str()).into_pyresult()?,
        })
    }

    fn sql_statements(&self) -> PyResult<Vec<String>> {
        self.inner.build_create_statements(&None).into_pyresult()
    }
}

lazy_static! {
    static ref RE_TEMPORAL_PARTITIONING: Regex =
        Regex::new(r"^(([0-9]+)\s*)?([a-zA-Z]+)$").unwrap();
}

#[pyclass]
pub struct CompactedTableSchemaBuilder {
    table_name: String,
    table_engine: Option<TableEngine>,
    compression_method: Option<CompressionMethod>,
    h3_base_resolutions: Option<Vec<u8>>,
    use_compaction: bool,
    temporal_resolution: Option<TemporalResolution>,
    temporal_partitioning: Option<TemporalPartitioning>,
    partition_by: Option<Vec<String>>,
    columns: Vec<(String, ColumnDefinition)>,
}

#[pymethods]
impl CompactedTableSchemaBuilder {
    #[new]
    fn new(table_name: String) -> Self {
        Self {
            table_name,
            table_engine: None,
            compression_method: None,
            h3_base_resolutions: None,
            use_compaction: true,
            temporal_resolution: None,
            temporal_partitioning: None,
            partition_by: None,
            columns: vec![],
        }
    }

    #[args(column_names = "None")]
    fn table_engine(
        &mut self,
        engine_name: String,
        column_names: Option<Vec<String>>,
    ) -> PyResult<()> {
        self.table_engine = Some(match engine_name.to_lowercase().as_str() {
            "replacingmergetree" => TableEngine::ReplacingMergeTree,
            "aggregatingmergetree" => TableEngine::AggregatingMergeTree,
            "summingmergetree" => {
                if let Some(sum_column_names) = column_names {
                    TableEngine::SummingMergeTree(sum_column_names)
                } else {
                    return Err(PyValueError::new_err("names of columns are required"));
                }
            }
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Unsupported table engine: {}",
                    engine_name
                )))
            }
        });
        Ok(())
    }

    #[args(level = "None")]
    fn compression_method(&mut self, method_name: String, level: Option<u8>) -> PyResult<()> {
        self.compression_method = Some(match method_name.to_lowercase().as_str() {
            "lz4hc" => CompressionMethod::LZ4HC(level.unwrap_or(9)),
            "zstd" => CompressionMethod::ZSTD(level.unwrap_or(6)),
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Unsupported compression method: {}",
                    method_name
                )))
            }
        });
        Ok(())
    }

    fn use_compacted_resolutions(&mut self, use_compaction: bool) {
        self.use_compaction = use_compaction;
    }

    #[args(compacted = "false")]
    fn h3_base_resolutions(&mut self, res: Vec<u8>) {
        self.h3_base_resolutions = Some(res)
    }

    #[args(order_key_position = "None")]
    fn add_column(
        &mut self,
        column_name: String,
        datatype_str: String,
        order_key_position: Option<u8>,
    ) -> PyResult<()> {
        let sc = SimpleColumn::new(datatype_from_string(datatype_str)?, order_key_position);
        self.columns
            .push((column_name, ColumnDefinition::Simple(sc)));
        Ok(())
    }

    fn add_h3index_column(&mut self, column_name: String) {
        self.columns.push((column_name, ColumnDefinition::H3Index));
    }

    ///
    ///
    /// The `min`, `max` and `avg` aggregations only work on the cells included in the data. Are
    ///  not all child-cells included, the missing ones are simply omitted and not assumed to be `0`.
    #[args(order_key_position = "None")]
    fn add_aggregated_column(
        &mut self,
        column_name: String,
        datatype_str: String,
        agg_method_str: String,
        order_key_position: Option<u8>,
    ) -> PyResult<()> {
        let sc = SimpleColumn::new(datatype_from_string(datatype_str)?, order_key_position);
        let agg = match agg_method_str.to_lowercase().as_str() {
            "sum" => AggregationMethod::Sum,
            "min" => AggregationMethod::Min,
            "max" => AggregationMethod::Max,
            "avg" | "average" => AggregationMethod::Average,
            "relativetoarea" | "relativetocellarea" => AggregationMethod::RelativeToCellArea,
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Unsupported aggregation method: {}",
                    agg_method_str
                )))
            }
        };
        self.columns
            .push((column_name, ColumnDefinition::WithAggregation(sc, agg)));
        Ok(())
    }

    fn temporal_resolution(&mut self, name: String) -> PyResult<()> {
        self.temporal_resolution = Some(match name.to_lowercase().as_str() {
            "second" | "seconds" => TemporalResolution::Second,
            "day" | "days" => TemporalResolution::Day,
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Unsupported temporal resolution: {}",
                    name
                )))
            }
        });
        Ok(())
    }

    fn temporal_partitioning(&mut self, name: String) -> PyResult<()> {
        let cap = RE_TEMPORAL_PARTITIONING.captures(&name).ok_or_else(|| {
            PyValueError::new_err(format!("Invalid temporal partitioning given: '{}'", name))
        })?;

        let unit_string = cap
            .get(3)
            .map(|s| s.as_str().to_string().to_lowercase())
            .unwrap_or_else(|| "".to_string());
        self.temporal_partitioning = Some(match unit_string.as_str() {
            "month" | "months" => TemporalPartitioning::Month,
            "year" | "years" => {
                let num_years: u8 = cap
                    .get(2)
                    .map(|s| {
                        s.as_str().parse().map_err(|e: ParseIntError| {
                            PyValueError::new_err(format!(
                                "Invalid number of years in temporal partitioning: {}",
                                e
                            ))
                        })
                    })
                    .unwrap_or(Ok(1_u8))?;
                TemporalPartitioning::Years(num_years)
            }
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Invalid temporal partitioning time unit given: '{}'",
                    unit_string
                )))
            }
        });
        Ok(())
    }

    fn partition_by(&mut self, column_names: Vec<String>) {
        self.partition_by = Some(column_names)
    }

    fn build(&self) -> PyResult<CompactedTableSchema> {
        let mut builder =
            h3cellstore::clickhouse::compacted_tables::schema::CompactedTableSchemaBuilder::new(
                &self.table_name,
            );

        if let Some(te) = &self.table_engine {
            builder = builder.table_engine(te.clone())
        }
        if let Some(cm) = &self.compression_method {
            builder = builder.compression_method(cm.clone())
        }
        builder = builder.use_compacted_resolutions(self.use_compaction);
        if let Some(h3res) = &self.h3_base_resolutions {
            builder = builder.h3_base_resolutions(h3res.clone())
        }
        if let Some(tr) = &self.temporal_resolution {
            builder = builder.temporal_resolution(tr.clone());
        }
        if let Some(tp) = &self.temporal_partitioning {
            builder = builder.temporal_partitioning(tp.clone());
        }
        if let Some(pb) = &self.partition_by {
            builder = builder.partition_by(pb.clone())
        }
        for (col_name, col_def) in self.columns.iter() {
            builder = builder.add_column(col_name.as_str(), col_def.clone())
        }

        let inner_schema = builder.build().into_pyresult()?;
        Ok(CompactedTableSchema {
            inner: inner_schema,
        })
    }
}

fn datatype_from_string(datatype_string: String) -> PyResult<ClickhouseDataType> {
    // todo: implement FromStr and use that instead of serde
    serde_json::from_str(&format!("\"{}\"", datatype_string))
        .map_err(|_e| PyValueError::new_err(format!("Unknown datatype: {}", datatype_string)))
}

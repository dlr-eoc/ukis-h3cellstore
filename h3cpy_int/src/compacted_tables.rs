use std::collections::{HashMap, HashSet};

use h3ron::{
    H3_MIN_RESOLUTION,
    index::Index,
};
use ndarray::ArrayView1;
use regex::Regex;

use crate::error::{check_index_valid, Error};

#[derive(Clone, Eq, PartialEq)]
pub struct TableSpec {
    pub h3_resolution: u8,
    pub is_compacted: bool,

    /// intermediate tables are just used during ingestion of new data
    /// into the clickhouse db
    pub is_intermediate: bool,
}

#[derive(Clone, Eq, PartialEq)]
pub struct Table {
    pub basename: String,
    pub spec: TableSpec,
}

lazy_static! {
  static ref RE_TABLE: Regex = {
      Regex::new(r"^([a-zA-Z].[a-zA-Z_0-9]+)_([0-9]{2})_(base|compacted)$").unwrap()
  };
}


impl Table {
    pub fn parse(full_table_name: &str) -> Option<Table> {
        if let Some(captures) = RE_TABLE.captures(full_table_name) {
            Some(Table {
                basename: captures[1].to_string(),
                spec: TableSpec {
                    h3_resolution: captures[2].parse().unwrap(),
                    is_compacted: captures[3] == *"compacted",
                    is_intermediate: false,
                },
            })
        } else {
            None
        }
    }

    pub fn to_table_name(&self) -> String {
        format!("{}_{:02}_{}", self.basename, self.spec.h3_resolution,
                if self.spec.is_compacted { "compacted" } else { "base" })
    }
}

#[derive(Clone)]
pub struct TableSet {
    pub basename: String,
    pub compacted_h3_resolutions: HashSet<u8>,
    pub base_h3_resolutions: HashSet<u8>,
    pub columns: HashMap<String, String>,
}

impl TableSet {
    fn new(basename: &str) -> TableSet {
        TableSet {
            basename: basename.to_string(),
            compacted_h3_resolutions: Default::default(),
            base_h3_resolutions: Default::default(),
            columns: Default::default(),
        }
    }

    pub fn compacted_tables(&self) -> Vec<Table> {
        let mut tables = Vec::new();
        for cr in self.compacted_h3_resolutions.iter() {
            let t = Table {
                basename: self.basename.clone(),
                spec: TableSpec {
                    is_compacted: true,
                    h3_resolution: *cr,
                    is_intermediate: false,
                },
            };
            tables.push(t);
        }
        tables
    }

    pub fn base_tables(&self) -> Vec<Table> {
        let mut tables = Vec::new();
        for cr in self.base_h3_resolutions.iter() {
            let t = Table {
                basename: self.basename.clone(),
                spec: TableSpec {
                    is_compacted: false,
                    h3_resolution: *cr,
                    is_intermediate: false,
                },
            };
            tables.push(t);
        }
        tables
    }

    pub fn tables(&self) -> Vec<Table> {
        let mut tables = self.base_tables();
        tables.append(&mut self.compacted_tables());
        tables
    }

    pub fn num_tables(&self) -> usize {
        self.base_h3_resolutions.len() + self.compacted_h3_resolutions.len()
    }

    pub fn build_select_query(&self, h3indexes_view: &ArrayView1<u64>) -> Result<String, Error> {
        // use the h3 resolution of the first index as the target resolution
        let h3_resolution = if let Some(h3index) = h3indexes_view.first() {
            let index = Index::from(*h3index);
            check_index_valid(&index)?;
            index.resolution()
        } else {
            return Err(Error::EmptyIndexes);
        };

        // collect the indexes and the parents (where the tables exist)
        let mut queryable_h3indexes: HashMap<_, HashSet<_>> = self.base_h3_resolutions.iter()
            .chain(self.compacted_h3_resolutions.iter())
            .filter(|r| **r <= h3_resolution)
            .map(|r| (*r, HashSet::new()))
            .collect();
        for h3index in h3indexes_view {
            let index = Index::from(*h3index);
            check_index_valid(&index)?;
            if index.resolution() != h3_resolution {
                return Err(Error::MixedResolutions);
            }
            queryable_h3indexes.iter_mut().for_each(|(r, r_indexes)| {
                r_indexes.insert(index.get_parent(*r).h3index());
            })
        }
        if queryable_h3indexes.is_empty() {
            return Err(Error::NoQueryableTables);
        }

        let query_string = {
            let selectable_columns = itertools::join(
                self.columns.iter()
                    .map(|(col_name, _)| col_name)
                    .filter(|col_name| !col_name.starts_with("h3index")),
                ", ",
            );


            let mut query_string_parts = Vec::new();
            for r in H3_MIN_RESOLUTION..=h3_resolution {
                if let Some(query_h3indexes) = queryable_h3indexes.get(&r) {
                    let query_h3indexes_string = itertools::join(
                        query_h3indexes.iter().map(|hi| hi.to_string()),
                        ",",
                    );

                    if r == h3_resolution {
                        let tablename = Table {
                            basename: self.basename.clone(),
                            spec: TableSpec {
                                h3_resolution: r,
                                is_compacted: false,
                                is_intermediate: false,
                            },
                        }.to_table_name();

                        query_string_parts.push(format!(
                            "select h3index, {} from {} where h3index in [{}]",
                            selectable_columns,
                            tablename,
                            query_h3indexes_string
                        ))
                    } else if r <= h3_resolution {
                        let tablename = Table {
                            basename: self.basename.clone(),
                            spec: TableSpec {
                                h3_resolution: r,
                                is_compacted: true,
                                is_intermediate: false,
                            },
                        }.to_table_name();

                        query_string_parts.push(format!(
                            "select h3ToParent(h3index, {}) as h3index, {} from {} where h3index in [{}]",
                            h3_resolution,
                            selectable_columns,
                            tablename,
                            query_h3indexes_string
                        ))
                    }
                }
            }

            // wrap with an outer select to remove indexes from formerly compacted parent resolutions
            // located outside the given h3indexes
            format!(
                "select * from ({}) f_wrap where h3index in [{}]",
                itertools::join(query_string_parts.iter(), " union all "),
                itertools::join(h3indexes_view.iter().map(|hi| hi.to_string()), ", ")
            )
        };
        Ok(query_string)
    }
}


/// identify the tablesets from a slice of tablenames
pub fn find_tablesets(tablenames: &[String]) -> HashMap<String, TableSet> {
    let mut tablesets = HashMap::default();

    for tablename in tablenames.iter() {
        if let Some(table) = Table::parse(tablename) {
            let tableset = tablesets.entry(table.basename.clone()).or_insert_with(|| {
                TableSet::new(&table.basename)
            });
            if table.spec.is_compacted {
                tableset.compacted_h3_resolutions.insert(table.spec.h3_resolution);
            } else {
                tableset.base_h3_resolutions.insert(table.spec.h3_resolution);
            }
        }
    }
    tablesets
}


#[cfg(test)]
mod tests {
    use crate::compacted_tables::{find_tablesets, Table, TableSpec};

    #[test]
    fn test_table_to_name() {
        let table = Table {
            basename: "some_table".to_string(),
            spec: TableSpec {
                h3_resolution: 5,
                is_compacted: false,
                is_intermediate: false,
            },
        };

        assert_eq!(table.to_table_name(), "some_table_05_base")
    }

    #[test]
    fn test_table_from_name() {
        let table = Table::parse("some_ta78ble_05_base");
        assert!(table.is_some());
        let table_u = table.unwrap();
        assert_eq!(table_u.basename, "some_ta78ble".to_string());
        assert_eq!(table_u.spec.h3_resolution, 5_u8);
        assert_eq!(table_u.spec.is_compacted, false);
    }

    #[test]
    fn test_find_tablesets() {
        let table_names = ["aggregate_function_combinators", "asynchronous_metrics", "build_options", "clusters",
            "collations", "columns", "contributors",
            "something_else_06_base", "something_else_07_base",
            "data_type_families", "databases", "detached_parts", "dictionaries", "disks", "events", "formats", "functions", "graphite_retentions",
            "macros", "merge_tree_settings", "merges", "metric_log", "metrics", "models", "mutations", "numbers", "numbers_mt", "one", "parts",
            "parts_columns", "processes", "quota_usage", "quotas", "replicas", "replication_queue", "row_policies", "settings", "stack_trace",
            "storage_policies", "table_engines", "table_functions", "tables", "trace_log", "zeros", "zeros_mt", "water_00_base", "water_00_compacted",
            "water_01_base", "water_01_compacted", "water_02_base", "water_02_compacted", "water_03_base", "water_03_compacted", "water_04_base",
            "water_04_compacted", "water_05_base", "water_05_compacted", "water_06_base", "water_06_compacted", "water_07_base", "water_07_compacted",
            "water_08_base", "water_08_compacted", "water_09_base", "water_09_compacted", "water_10_base", "water_10_compacted", "water_11_base",
            "water_11_compacted", "water_12_base", "water_12_compacted", "water_13_base", "water_13_compacted"
        ];

        let tablesets = find_tablesets(&table_names);
        assert_eq!(tablesets.len(), 2);
        assert!(tablesets.contains_key("water"));
        let water_ts = tablesets.get("water").unwrap();
        assert_eq!(water_ts.basename, "water");
        for h3res in 0..=13 {
            assert!(water_ts.base_h3_resolutions.contains(&h3res));
            assert!(water_ts.compacted_h3_resolutions.contains(&h3res));
        }
        assert!(!water_ts.base_h3_resolutions.contains(&14));
        assert!(!water_ts.compacted_h3_resolutions.contains(&14));

        assert!(tablesets.contains_key("something_else"));
        let se_ts = tablesets.get("something_else").unwrap();
        assert_eq!(se_ts.basename, "something_else");
        assert_eq!(se_ts.base_h3_resolutions.len(), 2);
        assert!(se_ts.base_h3_resolutions.contains(&6));
        assert!(se_ts.base_h3_resolutions.contains(&7));
        assert_eq!(se_ts.compacted_h3_resolutions.len(), 0);
    }
}
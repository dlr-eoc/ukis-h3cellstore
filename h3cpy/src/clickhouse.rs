use std::collections::HashMap;

use geo::algorithm::intersects::Intersects;
use h3ron::{Index, ToPolygon};
use numpy::{IntoPyArray, PyArray1, PyReadonlyArray1};
use pyo3::{prelude::*, PyResult, Python};

use h3cpy_int::compacted_tables::TableSetQuery;
use h3cpy_int::ColVec;

use crate::syncapi::ClickhousePool;
use crate::{
    inspect::TableSet as TableSetWrapper,
    pywrap::{check_index_valid, intresult_to_pyresult, Polygon},
    window::{create_window, SlidingH3Window},
};

#[pyclass]
pub struct ClickhouseConnection {
    pub(crate) clickhouse_pool: ClickhousePool,
}

impl ClickhouseConnection {
    pub fn new(clickhouse_pool: ClickhousePool) -> Self {
        Self { clickhouse_pool }
    }
}

#[pymethods]
impl ClickhouseConnection {
    #[args(querystring_template = "None", prefetch_querystring_template = "None")]
    pub fn make_sliding_window(
        &self,
        window_polygon: &Polygon,
        tableset: &TableSetWrapper,
        target_h3_resolution: u8,
        window_max_size: u32,
        querystring_template: Option<String>,
        prefetch_querystring_template: Option<String>,
    ) -> PyResult<SlidingH3Window> {
        create_window(
            window_polygon.inner.clone(),
            &tableset.inner,
            target_h3_resolution,
            window_max_size,
            if let Some(s) = querystring_template {
                TableSetQuery::TemplatedSelect(s)
            } else {
                TableSetQuery::AutoGenerated
            },
            prefetch_querystring_template.map(|s| TableSetQuery::TemplatedSelect(s)),
        )
    }

    fn list_tablesets(&mut self) -> PyResult<HashMap<String, TableSetWrapper>> {
        Ok(self
            .clickhouse_pool
            .list_tablesets()?
            .drain()
            .map(|(k, v)| (k, TableSetWrapper { inner: v }))
            .collect())
    }

    fn fetch_query(&mut self, query_string: String) -> PyResult<ResultSet> {
        let resultset = self.clickhouse_pool.query_all(query_string)?.into();
        Ok(resultset)
    }

    #[args(querystring_template = "None")]
    fn fetch_tableset(
        &mut self,
        tableset: &TableSetWrapper,
        h3indexes: PyReadonlyArray1<u64>,
        querystring_template: Option<String>,
    ) -> PyResult<ResultSet> {
        let h3indexes_vec = h3indexes.as_array().to_vec();
        let query_string = intresult_to_pyresult(
            tableset
                .inner
                .build_select_query(&h3indexes_vec, &querystring_template.into()),
        )?;

        let mut resultset: ResultSet = self
            .clickhouse_pool
            .query_all_with_uncompacting(query_string, h3indexes_vec.iter().cloned().collect())?
            .into();
        resultset.h3indexes_queried = Some(h3indexes_vec);
        Ok(resultset)
    }

    /// check if the tableset contains the h3index or any of its parents
    #[args(querystring_template = "None")]
    fn has_data(
        &mut self,
        tableset: &TableSetWrapper,
        h3index: u64,
        querystring_template: Option<String>,
    ) -> PyResult<bool> {
        let index = Index::from(h3index);
        check_index_valid(&index)?;

        let tablesetquery = match querystring_template {
            Some(qs) => TableSetQuery::TemplatedSelect(format!("{} limit 1", qs)),
            None => TableSetQuery::TemplatedSelect(
                "select h3index from <[table]> where h3index in <[h3indexes]> limit 1".to_string(),
            ),
        };
        let query_string = intresult_to_pyresult(
            tableset
                .inner
                .build_select_query(&[index.h3index()], &tablesetquery),
        )?;
        self.clickhouse_pool.query_returns_rows(query_string)
    }

    pub fn fetch_next_window(
        &mut self,
        sliding_h3_window: &mut SlidingH3Window,
        tableset: &TableSetWrapper,
    ) -> PyResult<Option<ResultSet>> {
        while let Some(window_h3index) = sliding_h3_window.next_window() {
            // check if the window index contains any data on coarse resolution, when not,
            // then there is no need to load anything
            if !self.has_data(
                tableset,
                window_h3index,
                sliding_h3_window.prefetch_query.clone().into(),
            )? {
                log::info!("window without any database contents skipped");
                continue;
            }

            let child_indexes: Vec<_> = Index::from(window_h3index)
                .get_children(sliding_h3_window.target_h3_resolution)
                .drain(..)
                // remove children located outside of the window_polygon. It is probably is not
                // worth the effort, but it allows to relocate some load from the DB server
                // to the users machine.
                .filter(|ci| {
                    let p = ci.to_polygon();
                    sliding_h3_window.window_polygon.intersects(&p)
                })
                .map(|i| i.h3index())
                .collect();

            if child_indexes.is_empty() {
                log::info!("window without intersecting h3indexes skipped");
                continue;
            }

            let query_string = intresult_to_pyresult(
                tableset
                    .inner
                    .build_select_query(&child_indexes, &sliding_h3_window.query),
            )?;
            let mut resultset: ResultSet = self
                .clickhouse_pool
                .query_all_with_uncompacting(query_string, child_indexes.iter().cloned().collect())?
                .into();
            resultset.h3indexes_queried = Some(child_indexes);
            resultset.window_h3index = Some(window_h3index);

            return Ok(Some(resultset));
        }
        Ok(None)
    }
}

#[pyclass]
pub struct ResultSet {
    h3indexes_queried: Option<Vec<u64>>,
    window_h3index: Option<u64>,
    pub(crate) column_data: HashMap<String, ColVec>,
}

#[pymethods]
impl ResultSet {
    pub fn is_empty(&self) -> bool {
        if self.column_data.is_empty() {
            return true;
        }
        for (_, v) in self.column_data.iter() {
            if !v.is_empty() {
                return false;
            }
        }
        true
    }
}

impl From<HashMap<String, ColVec>> for ResultSet {
    fn from(column_data: HashMap<String, ColVec>) -> Self {
        Self {
            h3indexes_queried: None,
            window_h3index: None,
            column_data,
        }
    }
}

#[pymethods]
impl ResultSet {
    /// get the number of h3indexes which where used in the query
    #[getter]
    fn get_num_h3indexes_queried(&self) -> Option<usize> {
        match &self.h3indexes_queried {
            Some(a) => Some(a.len()),
            None => None,
        }
    }

    /// get the h3indexes which where used in the query as a numpy array
    #[getter]
    fn get_h3indexes_queried(&self, py: Python) -> Py<PyArray1<u64>> {
        let h3vec = match &self.h3indexes_queried {
            Some(a) => a.clone(),
            None => vec![],
        };
        h3vec.into_pyarray(py).to_owned()
    }

    /// get the h3index of the window in case this resultset was fetched in a
    /// sliding window
    #[getter]
    fn get_window_index(&self) -> PyResult<Option<u64>> {
        Ok(self.window_h3index)
    }

    #[getter]
    fn get_column_types(&self) -> PyResult<HashMap<String, String>> {
        Ok(self
            .column_data
            .iter()
            .map(|(name, data)| (name.clone(), data.type_name().to_string()))
            .collect())
    }
}

use std::collections::{HashMap, HashSet};

use geo::algorithm::intersects::Intersects;
use h3ron::{
    index::Index,
};
use numpy::{IntoPyArray, PyReadonlyArray1};
use pyo3::{
    prelude::*,
    PyResult,
    Python,
    exceptions::PyValueError,
};

use h3cpy_int::clickhouse::{
    ColVec,
    query::{
        list_tablesets,
        query_all,
        query_all_with_uncompacting,
        query_returns_rows
    },
};

use crate::{clickhouse::{
    ch_to_pyresult,
    RuntimedPool,
}, geometry::polygon_from_python, inspect::TableSet as TableSetWrapper, intresult_to_pyresult, window::{
    create_window,
    SlidingH3Window,
},
};

#[inline]
fn check_index_valid(index: &Index) -> PyResult<()> {
    if !index.is_valid() {
        Err(PyValueError::new_err(format!("invalid h3index given: {}", index.h3index())))
    } else {
        Ok(())
    }
}

#[pyclass]
pub struct ClickhouseConnection {
    pub(crate) rp: RuntimedPool,
}

#[pymethods]
impl ClickhouseConnection {
    pub fn make_sliding_window(&self, window_poly_like: &PyAny, tableset: &TableSetWrapper, target_h3_resolution: u8, window_max_size: u32) -> PyResult<SlidingH3Window> {
        let window_polygon = polygon_from_python(window_poly_like)?;
        create_window(window_polygon, &tableset.inner, target_h3_resolution, window_max_size)
    }

    fn list_tablesets(&mut self) -> PyResult<HashMap<String, TableSetWrapper>> {
        let client = self.rp.get_client()?;
        let mut ts = ch_to_pyresult(self.rp.rt.block_on(async {
            list_tablesets(client).await
        }))?;
        Ok(ts.drain()
            .map(|(k, v)| (k, TableSetWrapper { inner: v }))
            .collect()
        )
    }

    fn fetch_query(&mut self, query_string: String) -> PyResult<ResultSet> {
        let client = self.rp.get_client()?;
        let column_data = ch_to_pyresult(self.rp.rt.block_on(async {
            query_all(client, query_string).await
        }))?;
        Ok(create_resultset(column_data))
    }

    fn fetch_tableset(&mut self, tableset: &TableSetWrapper, h3indexes: PyReadonlyArray1<u64>) -> PyResult<ResultSet> {
        let h3indexes_view = h3indexes.as_array();
        let query_string = intresult_to_pyresult(tableset.inner.build_select_query(&h3indexes_view, false))?;

        let client = self.rp.get_client()?;
        let column_data = ch_to_pyresult(self.rp.rt.block_on(async {
            let h3index_set: HashSet<_> = h3indexes_view.iter().cloned().collect();
            query_all_with_uncompacting(client, query_string, h3index_set).await
        }))?;
        let mut resultset = create_resultset(column_data);
        resultset.num_h3indexes_queried = Some(h3indexes_view.len());
        Ok(resultset)
    }

    /// check if the tableset contains the h3index or any of its parents
    fn has_data(&mut self, tableset: &TableSetWrapper, h3index: u64) -> PyResult<bool> {
        let index = Index::from(h3index);
        check_index_valid(&index)?;

        let mut queries = vec![];
        tableset.inner.tables().iter().for_each(|t| {
            if (!t.spec.is_compacted && t.spec.h3_resolution == index.resolution()) || (t.spec.is_compacted && t.spec.h3_resolution <= index.resolution()) {
                queries.push(format!(
                    "select h3index from {} where h3index = {} limit 1",
                    t.to_table_name(),
                    index.get_parent(t.spec.h3_resolution).h3index()
                ))
            }
        });
        if queries.is_empty() {
            return Ok(false);
        }

        let client = self.rp.get_client()?;
        ch_to_pyresult(self.rp.rt.block_on(async {
            query_returns_rows(client, itertools::join(queries, " union all ")).await
        }))
    }


    pub fn fetch_next_window(&mut self, py: Python<'_>, sliding_h3_window: &mut SlidingH3Window, tableset: &TableSetWrapper) -> PyResult<Option<ResultSet>> {
        while let Some(window_h3index) = sliding_h3_window.next_window() {
            // check if the window index contains any data on coarse resolution, when not,
            // then there is no need to load anything
            if !self.has_data(tableset, window_h3index)? {
                log::info!("window without any database contents skipped");
                continue;
            }

            let child_indexes: Vec<_> = Index::from(window_h3index)
                .get_children(sliding_h3_window.target_h3_resolution)
                .drain(..)
                // remove children located outside the window_polygon. It is probably is not worth the effort,
                // but it allows to relocate some load to the client.
                .filter(|ci| {
                    let p = ci.polygon();
                    sliding_h3_window.window_polygon.intersects(&p)
                })
                .map(|i| i.h3index())
                .collect();
            // TODO: add window index to resultset
            return Ok(Some(self.fetch_tableset(tableset, child_indexes.into_pyarray(py).readonly())?));
        }
        Ok(None)
    }
}


#[pyclass]
pub struct ResultSet {
    num_h3indexes_queried: Option<usize>,
    pub(crate) column_data: HashMap<String, ColVec>,
}

fn create_resultset(column_data: HashMap<String, ColVec>) -> ResultSet {
    ResultSet {
        num_h3indexes_queried: None,
        column_data,
    }
}


#[pymethods]
impl ResultSet {
    #[getter]
    fn get_num_h3indexes_queried(&self) -> PyResult<Option<usize>> {
        Ok(self.num_h3indexes_queried)
    }

    #[getter]
    fn get_column_types(&self) -> PyResult<HashMap<String, String>> {
        Ok(self.column_data.iter()
            .map(|(name, data)| (name.clone(), data.type_name().to_string()))
            .collect()
        )
    }
}

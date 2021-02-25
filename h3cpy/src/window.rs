use std::collections::{HashSet, VecDeque};

use geo::algorithm::intersects::Intersects;
use geo::algorithm::{bounding_rect::BoundingRect, centroid::Centroid};
use geo_types::{Polygon, Rect};
use h3ron::{polyfill, Index, ToPolygon};
use h3ron_h3_sys::H3Index;
use pyo3::exceptions::PyRuntimeError;
use pyo3::{exceptions::PyValueError, prelude::*, PyResult};

use h3cpy_int::{
    compacted_tables::{TableSet, TableSetQuery},
    window::window_index_resolution,
    ColVec,
};

use crate::clickhouse::ResultSet;
use crate::pywrap::intresult_to_pyresult;
use crate::syncapi::ClickhousePool;

#[pyclass]
pub struct SlidingH3Window {
    pub window_polygon: Polygon<f64>,
    pub window_rect: Rect<f64>,
    pub target_h3_resolution: u8,
    window_indexes: Vec<Index>,
    iter_pos: usize,

    /// window indexes which have been pre-checked to contain data
    prefetched_window_indexes: VecDeque<Index>,

    pub(crate) tableset: TableSet,
    pub(crate) query: TableSetQuery,
    pub(crate) prefetch_query: TableSetQuery,
}

impl SlidingH3Window {
    pub fn create(
        window_polygon: Polygon<f64>,
        tableset: TableSet,
        target_h3_resolution: u8,
        window_max_size: u32,
        query: TableSetQuery,
        prefetch_query: Option<TableSetQuery>,
    ) -> PyResult<Self> {
        let window_rect = match window_polygon.bounding_rect() {
            Some(w) => w,
            None => return Err(PyValueError::new_err("empty polygon given")),
        };

        let window_res = window_index_resolution(&tableset, target_h3_resolution, window_max_size);
        log::info!(
            "sliding window: using H3 res {} as window resolution",
            window_res
        );

        let mut window_index_set = HashSet::new();
        let mut add_index = |index: Index| {
            // polyfill just uses the centroid to determinate if an index is convert,
            // but we also want intersecting h3 cells where the centroid may be outside
            // of the polygon, so we add the direct neighbors as well.
            for ring_h3index in index.k_ring(1) {
                window_index_set.insert(ring_h3index);
            }
            window_index_set.insert(index);
        };

        for h3index in polyfill(&window_polygon, window_res) {
            let index = Index::from(h3index);
            add_index(index);
        }

        // for small windows, polyfill may not yield results,
        // so just adding the center as well.
        if let Some(point) = window_polygon.centroid() {
            let index = Index::from_coordinate(&point.0, target_h3_resolution);
            add_index(index);
        }
        log::info!(
            "sliding window: {} window indexes found",
            window_index_set.len()
        );

        let prefetch_query_fallback = prefetch_query.unwrap_or_else(|| query.clone());
        Ok(Self {
            window_polygon,
            window_rect,
            target_h3_resolution,
            window_indexes: window_index_set.drain().collect(),
            iter_pos: 0,
            prefetched_window_indexes: Default::default(),
            tableset,
            query,
            prefetch_query: prefetch_query_fallback,
        })
    }
}

impl SlidingH3Window {
    /// prefetch until some data-containing indexes where found or the
    /// window has been completely crawled
    fn prefetch_next_window_indexes(&mut self, pool: &mut ClickhousePool) -> PyResult<()> {
        loop {
            // prefetch a new batch
            let mut indexes_to_prefetch = vec![];
            for _ in 0..20 {
                if let Some(window_index) = self.window_indexes.get(self.iter_pos) {
                    indexes_to_prefetch.push(window_index);
                    self.iter_pos += 1;
                } else {
                    break; // no more window_indexes available
                }
            }
            if indexes_to_prefetch.is_empty() {
                return Ok(()); // reached the end of the window iteration
            }

            let query_string = {
                let mut parts = vec![];
                for index in indexes_to_prefetch {
                    let q = intresult_to_pyresult(
                        self.tableset
                            .build_select_query(&[index.h3index()], &self.prefetch_query),
                    )?;
                    parts.push(format!("(select h3index from ({}) limit 1)", q));
                }
                parts.join("\n union all ")
            };

            let query_data = pool.query_all(query_string)?;
            if let Some(colvec) = query_data.get("h3index") {
                if colvec.is_empty() {
                    continue;
                }
                return match colvec {
                    ColVec::U64(h3indexes) => {
                        h3indexes.iter().for_each(|h3i| {
                            self.prefetched_window_indexes.push_back(Index::from(*h3i))
                        });
                        Ok(())
                    }
                    _ => Err(PyRuntimeError::new_err(
                        "expected the 'h3index' column of the prefetch query to be UInt64",
                    )),
                };
            } else {
                return Err(PyRuntimeError::new_err(
                    "expected the prefetch query to contain a column called 'h3index'",
                ));
            }
        }
    }

    fn next_window(&mut self, pool: &mut ClickhousePool) -> PyResult<Option<H3Index>> {
        // return and drain the prefetched ones first
        if let Some(window_index) = self.prefetched_window_indexes.pop_front() {
            return Ok(Some(window_index.h3index()));
        }
        self.prefetch_next_window_indexes(pool)?;

        if let Some(window_index) = self.prefetched_window_indexes.pop_front() {
            Ok(Some(window_index.h3index()))
        } else {
            Ok(None) // finished with window iteration
        }
    }

    pub fn fetch_next_window(&mut self, pool: &mut ClickhousePool) -> PyResult<Option<ResultSet>> {
        while let Some(window_h3index) = self.next_window(pool)? {
            let child_indexes: Vec<_> = Index::from(window_h3index)
                .get_children(self.target_h3_resolution)
                .drain(..)
                // remove children located outside of the window_polygon. It is probably is not
                // worth the effort, but it allows to relocate some load from the DB server
                // to the users machine.
                .filter(|ci| {
                    let p = ci.to_polygon();
                    self.window_polygon.intersects(&p)
                })
                .map(|i| i.h3index())
                .collect();

            if child_indexes.is_empty() {
                log::info!("window without intersecting h3indexes skipped");
                continue;
            }

            let query_string = intresult_to_pyresult(
                self.tableset
                    .build_select_query(&child_indexes, &self.query),
            )?;
            let mut resultset: ResultSet = pool
                .query_all_with_uncompacting(query_string, child_indexes.iter().cloned().collect())?
                .into();
            resultset.h3indexes_queried = Some(child_indexes);
            resultset.window_h3index = Some(window_h3index);

            return Ok(Some(resultset));
        }
        Ok(None)
    }
}

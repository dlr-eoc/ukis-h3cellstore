use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use h3ron::{Index, polyfill, ToPolygon};
use h3ron_h3_sys::H3Index;
use pyo3::{exceptions::PyRuntimeError, prelude::*, PyResult};
use tokio::task::JoinHandle as TaskJoinHandle;

use bamboo_h3_int::{
    ColVec,
    compacted_tables::{TableSet, TableSetQuery},
    geo::algorithm::{centroid::Centroid, intersects::Intersects},
    geo_types::Polygon,
    window::window_index_resolution,
};

use crate::clickhouse::ResultSet;
use crate::pywrap::intresult_to_pyresult;
use crate::syncapi::{ClickhousePool, Query};

#[pyclass]
pub struct SlidingH3Window {
    clickhouse_pool: Arc<ClickhousePool>,
    window_polygon: Polygon<f64>,
    target_h3_resolution: u8,
    window_h3_resolution: u8,
    window_indexes: Vec<Index>,
    iter_pos: usize,

    /// window indexes which have been pre-checked to contain data
    prefetched_window_indexes: VecDeque<Index>,

    tableset: TableSet,
    query: TableSetQuery,
    prefetch_query: TableSetQuery,
    preloaded_window: Option<PreloadedWindow>,
}

#[pymethods]
impl SlidingH3Window {
    fn fetch_next_window(&mut self) -> PyResult<Option<ResultSet>> {
        if let Some(preloaded) = self.preloaded_window.take() {
            let mut resultset: ResultSet =
                self.clickhouse_pool.await_query(preloaded.awaiting)?.into();
            resultset.h3indexes_queried = Some(preloaded.h3indexes_queried);
            resultset.window_h3index = Some(preloaded.window_h3index);

            next_window_preload(self)?;

            Ok(Some(resultset))
        } else if let Some(queryparameters) = next_window_queryparameters(self)? {
            let mut resultset: ResultSet =
                self.clickhouse_pool.query(queryparameters.query)?.into();
            resultset.h3indexes_queried = Some(queryparameters.h3indexes_queried);
            resultset.window_h3index = Some(queryparameters.window_h3index);

            Ok(Some(resultset))
        } else {
            Ok(None)
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn create_window(
    clickhouse_pool: Arc<ClickhousePool>,
    window_polygon: Polygon<f64>,
    tableset: TableSet,
    target_h3_resolution: u8,
    window_max_size: u32,
    query: TableSetQuery,
    prefetch_query: Option<TableSetQuery>,
) -> PyResult<SlidingH3Window> {
    let window_h3_resolution =
        window_index_resolution(&tableset, target_h3_resolution, window_max_size);
    log::info!(
        "sliding window: using H3 res {} as window resolution",
        window_h3_resolution
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

    for h3index in polyfill(&window_polygon, window_h3_resolution) {
        let index = Index::from(h3index);
        add_index(index);
    }

    // for small windows, polyfill may not yield results,
    // so just adding the center as well.
    if let Some(point) = window_polygon.centroid() {
        let index = Index::from_coordinate(&point.0, window_h3_resolution);
        add_index(index);
    }
    log::info!(
        "sliding window: {} window indexes found",
        window_index_set.len()
    );

    let prefetch_query_fallback = prefetch_query.unwrap_or_else(|| query.clone());
    let mut sliding_window = SlidingH3Window {
        clickhouse_pool,
        window_polygon,
        target_h3_resolution,
        window_h3_resolution,
        window_indexes: window_index_set.drain().collect(),
        iter_pos: 0,
        prefetched_window_indexes: Default::default(),
        tableset,
        query,
        prefetch_query: prefetch_query_fallback,
        preloaded_window: None,
    };

    next_window_preload(&mut sliding_window)?;

    Ok(sliding_window)
}

struct PreloadedWindow {
    awaiting: TaskJoinHandle<PyResult<HashMap<String, ColVec>>>,
    h3indexes_queried: Vec<u64>,
    window_h3index: u64,
}

fn next_window_preload(sliding_window: &mut SlidingH3Window) -> PyResult<()> {
    if let Some(queryparameters) = next_window_queryparameters(sliding_window)? {
        let preloaded = PreloadedWindow {
            awaiting: sliding_window
                .clickhouse_pool
                .spawn_query(queryparameters.query),

            h3indexes_queried: queryparameters.h3indexes_queried,
            window_h3index: queryparameters.window_h3index,
        };
        sliding_window.preloaded_window = Some(preloaded);
    }
    Ok(())
}

struct QueryParameters {
    query: Query,
    h3indexes_queried: Vec<u64>,
    window_h3index: u64,
}

fn next_window_queryparameters(
    sliding_window: &mut SlidingH3Window,
) -> PyResult<Option<QueryParameters>> {
    while let Some(window_h3index) = next_window_index(sliding_window)? {
        let child_indexes: Vec<_> = Index::from(window_h3index)
            .get_children(sliding_window.target_h3_resolution)
            .drain(..)
            // remove children located outside of the window_polygon. It is probably is not
            // worth the effort, but it allows to relocate some load from the DB server
            // to the users machine.
            .filter(|ci| {
                let p = ci.to_polygon();
                sliding_window.window_polygon.intersects(&p)
            })
            .map(|i| i.h3index())
            .collect();

        if child_indexes.is_empty() {
            log::info!("window without intersecting h3indexes skipped");
            continue;
        }

        let query_string = intresult_to_pyresult(
            sliding_window
                .tableset
                .build_select_query(&child_indexes, &sliding_window.query),
        )?;
        return Ok(Some(QueryParameters {
            query: Query::Uncompact(query_string, child_indexes.iter().cloned().collect()),
            h3indexes_queried: child_indexes,
            window_h3index,
        }));
    }
    Ok(None)
}

/// get the next window_index for the window
fn next_window_index(sliding_window: &mut SlidingH3Window) -> PyResult<Option<H3Index>> {
    // return and drain the prefetched ones first
    if let Some(window_index) = sliding_window.prefetched_window_indexes.pop_front() {
        return Ok(Some(window_index.h3index()));
    }
    prefetch_next_window_indexes(sliding_window)?;

    if let Some(window_index) = sliding_window.prefetched_window_indexes.pop_front() {
        Ok(Some(window_index.h3index()))
    } else {
        Ok(None) // finished with window iteration
    }
}

const WINDOW_INDEX_COL_NAME: &str = "window_index";

/// prefetch until some data-containing indexes where found, or the
/// window has been completely crawled
fn prefetch_next_window_indexes(sliding_window: &mut SlidingH3Window) -> PyResult<()> {
    loop {
        // prefetch a new batch
        let mut indexes_to_prefetch = vec![];
        for _ in 0..100 {
            if let Some(window_index) = sliding_window.window_indexes.get(sliding_window.iter_pos) {
                indexes_to_prefetch.push(window_index);
                sliding_window.iter_pos += 1;
            } else {
                break; // no more window_indexes available
            }
        }
        if indexes_to_prefetch.is_empty() {
            return Ok(()); // reached the end of the window iteration
        }

        let query_string = {
            let h3indexes: Vec<_> = indexes_to_prefetch.iter().map(|i| i.h3index()).collect();
            let q = intresult_to_pyresult(
                sliding_window
                    .tableset
                    .build_select_query(&h3indexes, &sliding_window.prefetch_query),
            )?;
            format!(
                "select distinct h3ToParent(h3index, {}) as {} from ({})",
                sliding_window.window_h3_resolution, WINDOW_INDEX_COL_NAME, q
            )
        };

        let query_data = sliding_window
            .clickhouse_pool
            .query(Query::Plain(query_string))?;
        if let Some(colvec) = query_data.get(WINDOW_INDEX_COL_NAME) {
            if colvec.is_empty() {
                continue;
            }
            return match colvec {
                ColVec::U64(h3indexes) => {
                    h3indexes.iter().for_each(|h3i| {
                        sliding_window
                            .prefetched_window_indexes
                            .push_back(Index::from(*h3i))
                    });
                    Ok(())
                }
                _ => Err(PyRuntimeError::new_err(format!(
                    "expected the '{}' column of the prefetch query to be UInt64",
                    WINDOW_INDEX_COL_NAME
                ))),
            };
        } else {
            return Err(PyRuntimeError::new_err(format!(
                "expected the generated prefetch query to contain a column called '{}'",
                WINDOW_INDEX_COL_NAME
            )));
        }
    }
}

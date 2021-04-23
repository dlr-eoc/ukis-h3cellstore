use std::collections::{HashSet, VecDeque};
use std::convert::TryFrom;
use std::sync::Arc;

use h3ron::{polyfill, HasH3Index, Index, ToCoordinate};
use h3ron_h3_sys::H3Index;
use pyo3::{exceptions::PyRuntimeError, prelude::*, PyResult};

use bamboo_h3_int::clickhouse::compacted_tables::{TableSet, TableSetQuery};
use bamboo_h3_int::clickhouse::window::window_index_resolution;
use bamboo_h3_int::{
    geo::algorithm::{centroid::Centroid, intersects::Intersects},
    geo_types::Polygon,
    ColVec, COL_NAME_H3INDEX,
};

use crate::clickhouse::{AwaitableResultSet, ResultSet};
use crate::error::IntoPyResult;
use crate::syncapi::{ClickhousePool, Query};

#[pyclass]
pub struct SlidingH3Window {
    clickhouse_pool: Arc<ClickhousePool>,
    window_polygon: Polygon<f64>,
    target_h3_resolution: u8,
    window_indexes: Vec<Index>,
    iter_pos: usize,

    /// window indexes which have been pre-checked to contain data
    prefetched_window_indexes: VecDeque<Index>,

    tableset: TableSet,
    query: TableSetQuery,

    /// query to pre-evaluate if a window is worth fetching
    prefetch_query: TableSetQuery,
    preloaded_window: Option<ResultSet>,
}

#[pymethods]
impl SlidingH3Window {
    fn fetch_next_window(&mut self) -> PyResult<Option<ResultSet>> {
        if let Some(preloaded) = self.preloaded_window.take() {
            preload_next_resultset(self)?;
            Ok(Some(preloaded))
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
    if (target_h3_resolution as i16 - window_h3_resolution as i16).abs() <= 3 {
        log::warn!(
            "sliding window: using H3 res {} as window resolution to iterate over H3 res {} data. This is probably inefficient - try to increase window_max_size.",
            window_h3_resolution,
            target_h3_resolution
        );
    } else {
        log::info!(
            "sliding window: using H3 res {} as window resolution",
            window_h3_resolution
        );
    }

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
        let index = Index::try_from(h3index).into_pyresult()?;
        add_index(index);
    }

    // for small windows, polyfill may not yield results,
    // so just adding the center as well.
    if let Some(point) = window_polygon.centroid() {
        let index = Index::from_coordinate(&point.0, window_h3_resolution).into_pyresult()?;
        add_index(index);
    }
    log::info!(
        "sliding window: {} window indexes found",
        window_index_set.len()
    );

    let mut window_indexes: Vec<_> = window_index_set.drain().collect();

    // always process windows in the same order. This is probably easier for to
    // user when inspecting the results produced during the processing
    window_indexes.sort_unstable();

    let prefetch_query_fallback = prefetch_query.unwrap_or_else(|| query.clone());
    let mut sliding_window = SlidingH3Window {
        clickhouse_pool,
        window_polygon,
        target_h3_resolution,
        window_indexes,
        iter_pos: 0,
        prefetched_window_indexes: Default::default(),
        tableset,
        query,
        prefetch_query: prefetch_query_fallback,
        preloaded_window: None,
    };

    preload_next_resultset(&mut sliding_window)?;

    Ok(sliding_window)
}

fn preload_next_resultset(sliding_window: &mut SlidingH3Window) -> PyResult<()> {
    if let Some(queryparameters) = next_window_queryparameters(sliding_window)? {
        let mut resultset: ResultSet = AwaitableResultSet::new(
            sliding_window.clickhouse_pool.clone(),
            queryparameters.query,
        )
        .into();
        resultset.window_h3index = Some(queryparameters.window_h3index);
        resultset.h3indexes_queried = Some(queryparameters.h3indexes_queried);
        sliding_window.preloaded_window = Some(resultset);
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
        let child_indexes: Vec<_> = Index::try_from(window_h3index)
            .into_pyresult()?
            .get_children(sliding_window.target_h3_resolution)
            .drain(..)
            // remove children located outside of the window_polygon. It is probably is not
            // worth the effort, but it allows to relocate some load from the DB server
            // to the users machine.
            .filter(|ci| {
                // using coordinate instead of the polygon to avoid having duplicated h3 cells
                // when window_polygon is a tile of a larger polygon. Using Index.to_polygon
                // would result in one line of h3 cells overlap between neighboring tiles.
                let p = ci.to_coordinate();
                sliding_window.window_polygon.intersects(&p)
            })
            .map(|i| i.h3index())
            .collect();

        if child_indexes.is_empty() {
            log::info!("window without intersecting h3indexes skipped");
            continue;
        }

        let query_string = sliding_window
            .tableset
            .build_select_query(&child_indexes, &sliding_window.query)
            .into_pyresult()?;
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

/// prefetch until some data-containing indexes where found, or the
/// window has been completely crawled
fn prefetch_next_window_indexes(sliding_window: &mut SlidingH3Window) -> PyResult<()> {
    loop {
        // prefetch a new batch
        let mut indexes_to_prefetch = vec![];
        for _ in 0..600 {
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

        let query = {
            let mut h3indexes: Vec<_> = indexes_to_prefetch.iter().map(|i| i.h3index()).collect();
            let q = sliding_window
                .tableset
                .build_select_query(&h3indexes, &sliding_window.prefetch_query)
                .into_pyresult()?;
            Query::Uncompact(
                format!("select distinct {} from ({})", COL_NAME_H3INDEX, q),
                h3indexes.drain(..).collect(),
            )
        };

        let query_data = sliding_window.clickhouse_pool.query(query)?;
        if let Some(colvec) = query_data.columns.get(COL_NAME_H3INDEX) {
            if colvec.is_empty() {
                continue;
            }
            return match colvec {
                ColVec::U64(h3indexes) => {
                    h3indexes.iter().for_each(|h3i| {
                        sliding_window
                            .prefetched_window_indexes
                            .push_back(Index::new(*h3i))
                    });
                    Ok(())
                }
                _ => Err(PyRuntimeError::new_err(format!(
                    "expected the '{}' column of the prefetch query to be UInt64",
                    COL_NAME_H3INDEX
                ))),
            };
        } else {
            return Err(PyRuntimeError::new_err(format!(
                "expected the generated prefetch query to contain a column called '{}'",
                COL_NAME_H3INDEX
            )));
        }
    }
}

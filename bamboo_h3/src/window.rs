use std::cmp::max;
use std::collections::{HashSet, VecDeque};
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::{Duration, Instant};

use either::Either;
use h3ron::{polyfill, H3Cell, Index, ToCoordinate};
use pyo3::{exceptions::PyRuntimeError, prelude::*, PyResult};

use bamboo_h3_int::clickhouse::compacted_tables::{TableSet, TableSetQuery};
use bamboo_h3_int::clickhouse::query::query_all_with_uncompacting;
use bamboo_h3_int::clickhouse::window::window_index_resolution;
use bamboo_h3_int::clickhouse_rs::ClientHandle;
use bamboo_h3_int::{
    geo::algorithm::{centroid::Centroid, intersects::Intersects},
    geo_types::Polygon,
    ColVec, ColumnSet, COL_NAME_H3INDEX,
};

use crate::clickhouse::ResultSet;
use crate::error::IntoPyResult;
use crate::syncapi::ClickhousePool;


pub struct SlidingWindowOptions {
    pub window_polygon: Polygon<f64>,
    pub target_h3_resolution: u8,
    pub window_max_size: u32,
    pub tableset: TableSet,
    pub query: TableSetQuery,

    /// query to pre-evaluate if a window is worth fetching
    pub prefetch_query: Option<TableSetQuery>,

    /// defines how many windows may be loaded in parallel.
    /// An increased number here also increase the memory requirements of the DB server.
    ///
    /// In most cases just using 1 is propably sufficient.
    pub concurrency: u8,
}

#[pyclass]
pub struct SlidingH3Window {
    clickhouse_pool: Arc<ClickhousePool>,
    rx_resultset: tokio::sync::mpsc::Receiver<PyResult<ResultSet>>,
    join_handle: Option<tokio::task::JoinHandle<PyResult<()>>>,
    shutdown: Arc<tokio::sync::Notify>,
}

#[pymethods]
impl SlidingH3Window {
    fn fetch_next_window(&mut self, py: Python) -> PyResult<Option<ResultSet>> {
        loop {
            let resultset_future = self.rx_resultset.recv();
            let resultset_recv_timeout = self.clickhouse_pool.runtime.block_on(async {
                tokio::time::timeout(Duration::from_millis(200), resultset_future).await
            });

            match resultset_recv_timeout {
                Ok(resultset_option) => {
                    return match resultset_option {
                        Some(rs) => rs.map(Some),
                        None => Ok(None),
                    }
                }

                Err(_elapsed) => {
                    // timeout reached. check if the python program has been interrupted
                    // and wait again if that was not the case
                    if let Err(e) = py.check_signals() {
                        self.shutdown.notify_waiters();
                        return Err(e);
                    }
                }
            }
        }
    }

    fn close(&mut self) -> PyResult<()> {
        self.finish_tasks()
    }
}

impl SlidingH3Window {
    fn finish_tasks(&mut self) -> PyResult<()> {
        self.shutdown.notify_waiters();

        // let all tasks collapse
        self.rx_resultset.close();

        if let Some(handle) = self.join_handle.take() {
            self.clickhouse_pool
                .runtime
                .block_on(async move { handle.await })
                .unwrap()
        } else {
            Ok(())
        }
    }
}

impl Drop for SlidingH3Window {
    fn drop(&mut self) {
        let _ = self.finish_tasks();
    }
}

impl SlidingH3Window {
    pub fn create(
        clickhouse_pool: Arc<ClickhousePool>,
        options: SlidingWindowOptions,
    ) -> PyResult<Self> {
        let window_h3_resolution = determinate_window_h3_resolution(
            &options.tableset,
            options.target_h3_resolution,
            options.window_max_size,
        );
        let window_indexes = build_window_indexes(&options.window_polygon, window_h3_resolution)?;

        // use a higher capacity to have a few available in case the consumer
        // of the sliding window sometimes discards single windows
        let resultset_capacity = max(3, options.concurrency as usize * 3);

        let (tx_resultset, rx_resultset) = clickhouse_pool
            .runtime
            .block_on(async { tokio::sync::mpsc::channel(resultset_capacity) });

        let shutdown = Arc::new(tokio::sync::Notify::new());
        let shutdown2 = shutdown.clone();

        let pool_copy = clickhouse_pool.clone();
        let join_handle = clickhouse_pool.runtime.spawn(async move {
            let shutdown_notified = shutdown2.notified();
            let window_iteration = launch_window_iteration(
                pool_copy,
                tx_resultset,
                options,
                window_indexes,
            );

            tokio::select! {
                _ = shutdown_notified => {
                    // shutdown requested
                    Ok(())
                }
                res = window_iteration => {
                    // window iteration finished
                    res
                }
            }
        });

        Ok(Self {
            clickhouse_pool,
            rx_resultset,
            join_handle: Some(join_handle),
            shutdown,
        })
    }
}

async fn launch_window_iteration(
    clickhouse_pool: Arc<ClickhousePool>,
    tx_resultset: tokio::sync::mpsc::Sender<PyResult<ResultSet>>,
    options: SlidingWindowOptions,
    window_indexes: VecDeque<H3Cell>,
) -> PyResult<()> {
    let options_arc = Arc::new(options);
    let (tx_window_index, rx_window_index) =
        async_channel::bounded(options_arc.concurrency as usize);

    let mut fetch_handles = vec![];
    for _ in 0..options_arc.concurrency {
        let client = clickhouse_pool.pool.get_handle().await.into_pyresult()?;
        let rx_window_index_ = rx_window_index.clone();
        let tx_resulset_ = tx_resultset.clone();
        let opts = options_arc.clone();
        let handle = tokio::task::spawn(async move {
            // fetch next window
            fetch_window(client, opts, rx_window_index_, tx_resulset_).await
        });
        fetch_handles.push(handle);
    }
    // close this tasks copy of the channel to leave no open copies once the tasks have finished.
    std::mem::drop(tx_resultset);
    std::mem::drop(rx_window_index);

    let prefetch_handle = {
        let client = clickhouse_pool.pool.get_handle().await.into_pyresult()?;
        let opts = options_arc.clone();
        tokio::task::spawn(async move {
            // check window indexes
            prefetch_window_indexes(client, window_indexes, opts, tx_window_index).await
        })
    };

    prefetch_handle.await.into_pyresult()??;
    for handle in fetch_handles.drain(..) {
        handle.await.into_pyresult()??;
    }
    Ok(())
}

fn determinate_window_h3_resolution(
    tableset: &TableSet,
    target_h3_resolution: u8,
    window_max_size: u32,
) -> u8 {
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
    window_h3_resolution
}

fn build_window_indexes(
    poly: &Polygon<f64>,
    window_h3_resolution: u8,
) -> PyResult<VecDeque<H3Cell>> {
    let mut window_index_set = HashSet::new();

    for h3index in polyfill(&poly, window_h3_resolution) {
        let index = H3Cell::try_from(h3index).into_pyresult()?;
        // polyfill just uses the centroid to determinate if an index is convert,
        // but we also want intersecting h3 cells where the centroid may be outside
        // of the polygon, so we add the direct neighbors as well.
        for ring_h3index in index.k_ring(1) {
            window_index_set.insert(ring_h3index);
        }
        window_index_set.insert(index);
    }

    // for small windows, polyfill may not yield results,
    // so just adding the center as well.
    if let Some(point) = poly.centroid() {
        let index = H3Cell::from_coordinate(&point.0, window_h3_resolution).into_pyresult()?;
        window_index_set.insert(index);
    }
    log::info!(
        "sliding window: {} window indexes found",
        window_index_set.len()
    );

    let mut window_indexes: Vec<_> = window_index_set.drain().collect();

    // always process windows in the same order. This is probably easier for to
    // user when inspecting the results produced during the processing
    window_indexes.sort_unstable();

    Ok(window_indexes.drain(..).collect())
}

/// prefetch until some data-containing indexes where found, or the
/// window has been completely crawled
async fn prefetch_window_indexes(
    mut client: ClientHandle,
    mut window_indexes: VecDeque<H3Cell>,
    options: Arc<SlidingWindowOptions>,
    tx_window_index: async_channel::Sender<H3Cell>,
) -> PyResult<()> {
    set_clickhouse_num_window_threads(&mut client).await?;

    loop {
        // prefetch a new batch
        let mut indexes_to_prefetch = vec![];
        for _ in 0..600 {
            if let Some(window_index) = window_indexes.pop_front() {
                indexes_to_prefetch.push(window_index);
            } else {
                break; // no more window_indexes available
            }
        }
        if indexes_to_prefetch.is_empty() {
            return Ok(()); // reached the end of the window iteration
        }

        let mut h3indexes: Vec<_> = indexes_to_prefetch.iter().map(|i| i.h3index()).collect();
        let q = options
            .tableset
            .build_select_query(
                &h3indexes,
                match &options.prefetch_query {
                    Some(pq) => pq,
                    None => &options.query,
                },
            )
            .into_pyresult()?;

        let window_h3indexes = {
            let columnset =
                query_all_with_uncompacting(&mut client, q, h3indexes.drain(..).collect())
                    .await
                    .into_pyresult()?;
            window_indexes_from_columnset(columnset)?
        };

        match window_h3indexes {
            Some(h3indexes) => {
                for h3index in h3indexes.iter() {
                    if tx_window_index.send(H3Cell::new(*h3index)).await.is_err() {
                        log::debug!("receivers for window indexes are gone");
                        return Ok(());
                    }
                }
            }
            None => continue,
        }
    }
}

fn window_indexes_from_columnset(mut columnset: ColumnSet) -> PyResult<Option<Vec<u64>>> {
    if let Some(colvec) = columnset.columns.remove(COL_NAME_H3INDEX) {
        if colvec.is_empty() {
            return Ok(None);
        }
        match colvec {
            ColVec::U64(mut h3indexes) => {
                // make the ordering more deterministic by sorting, deduplicate for safety in case
                // the prefetch query returns duplicates.
                h3indexes.sort_unstable();
                h3indexes.dedup();

                Ok(Some(h3indexes))
            }
            _ => Err(PyRuntimeError::new_err(format!(
                "expected the '{}' column of the prefetch query to be UInt64",
                COL_NAME_H3INDEX
            ))),
        }
    } else {
        Err(PyRuntimeError::new_err(format!(
            "expected the generated prefetch query to contain a column called '{}'",
            COL_NAME_H3INDEX
        )))
    }
}

async fn fetch_window(
    mut client: ClientHandle,
    options: Arc<SlidingWindowOptions>,
    rx_window_index: async_channel::Receiver<H3Cell>,
    tx_resultset: tokio::sync::mpsc::Sender<PyResult<ResultSet>>,
) -> PyResult<()> {
    set_clickhouse_num_window_threads(&mut client).await?;

    loop {
        let window_index = match rx_window_index.recv().await {
            Ok(wi) => wi,
            Err(_) => {
                log::debug!("sender for window index dropped");
                break;
            }
        };

        if tx_resultset.is_closed() {
            break;
        }

        log::debug!("fetching data for window {}", window_index.to_string());
        let child_indexes: Vec<_> = window_index
            .get_children(options.target_h3_resolution)
            .drain(..)
            // remove children located outside of the window_polygon. It is probably is not
            // worth the effort, but it allows to relocate some load from the DB server
            // to the users machine.
            .filter(|ci| {
                // using coordinate instead of the polygon to avoid having duplicated h3 cells
                // when window_polygon is a tile of a larger polygon. Using Index.to_polygon
                // would result in one line of h3 cells overlap between neighboring tiles.
                let p = ci.to_coordinate();
                options.window_polygon.intersects(&p)
            })
            .map(|i| i.h3index())
            .collect();

        if tx_resultset.is_closed() {
            break;
        }

        if child_indexes.is_empty() {
            log::debug!("window without intersecting h3indexes skipped");
            continue;
        }

        let query_string = options
            .tableset
            .build_select_query(&child_indexes, &options.query)
            .into_pyresult()?;

        let t_start = Instant::now();
        let resultset = query_all_with_uncompacting(
            &mut client,
            query_string,
            child_indexes.iter().cloned().collect(),
        )
        .await
        .into_pyresult()
        .map(|columnset| {
            ResultSet {
                h3indexes_queried: Some(child_indexes),
                window_h3index: Some(window_index.h3index()),
                column_data: Either::Left(Some(columnset.into())),
                query_duration: Some(t_start.elapsed()),
            }
        });

        if tx_resultset.send(resultset).await.is_err() {
            log::debug!("Receiver for window resultset dropped");
            return Ok(());
        }
    }
    Ok(())
}

/// use a low level of concurrency in clickhouse to keep the load and memory requirements
/// on the db server low. the fetch here happens ahead of time anyways.
/// related: https://github.com/ClickHouse/ClickHouse/issues/22980#issuecomment-818473308
///
/// The default number of threads according to the linked issue is 6.
async fn set_clickhouse_num_window_threads(client: &mut ClientHandle) -> PyResult<()> {
    set_clickhouse_threads(client, crate::env::window_num_clickhouse_threads()).await
}

async fn set_clickhouse_threads(client: &mut ClientHandle, n_threads: u8) -> PyResult<()> {
    client
        .execute(format!("set max_threads = {}", n_threads))
        .await
        .into_pyresult()
}

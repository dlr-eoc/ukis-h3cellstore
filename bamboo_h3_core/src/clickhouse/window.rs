///
/// a h3 window iterator for rust
///
///
use std::cmp::{max, Ordering};
use std::collections::{HashSet, VecDeque};
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clickhouse_rs::{ClientHandle, Pool};
use h3ron::{polyfill, H3Cell, Index, ToCoordinate};
use tracing::{debug, error, info, instrument, span, warn, Level};
use tracing_futures::Instrument;

use crate::clickhouse::compacted_tables::{TableSet, TableSetQuery};
use crate::clickhouse::query::{query_all_with_uncompacting, set_clickhouse_max_threads};
use crate::clickhouse::QueryOutput;
use crate::error::Error;
use crate::geo::algorithm::centroid::Centroid;
use crate::geo::algorithm::intersects::Intersects;
use crate::geo_types::Polygon;
use crate::{ColVec, ColumnSet, COL_NAME_H3INDEX};
use geo::Coordinate;

/// find the resolution generate coarser h3-indexes to access the tableset without needing to fetch more
/// than window_max_size indexes per batch.
///
/// That resolution must be a base resolution
pub fn window_index_resolution(
    table_set: &TableSet,
    target_h3_resolution: u8,
    window_max_size: u32,
) -> u8 {
    let mut resolutions: Vec<_> = table_set
        .base_resolutions()
        .iter()
        .filter(|r| **r < target_h3_resolution)
        .cloned()
        .collect();
    resolutions.sort_unstable();

    let mut window_h3_resolution = target_h3_resolution;
    for r in resolutions {
        let r_diff = (target_h3_resolution - r) as u32;
        if 7_u64.pow(r_diff) <= (window_max_size as u64) {
            window_h3_resolution = r;
            break;
        }
    }
    window_h3_resolution
}

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
    /// In most cases just using 1 is probably sufficient.
    pub concurrency: u8,

    /// use a low level of concurrency in clickhouse to keep the load and memory requirements
    /// on the db server low. the fetch here happens ahead of time anyways.
    /// related: https://github.com/ClickHouse/ClickHouse/issues/22980#issuecomment-818473308
    ///
    /// The default number of threads according to the linked issue is 6.
    pub window_num_clickhouse_threads: u8,
}

pub struct SlidingH3Window {
    rx_output: tokio::sync::mpsc::Receiver<Result<QueryOutput<ColumnSet>, Error>>,
    join_handle: Option<tokio::task::JoinHandle<Result<(), Error>>>,
    shutdown: Arc<tokio::sync::Notify>,
}

impl SlidingH3Window {
    pub async fn create(pool: Arc<Pool>, options: SlidingWindowOptions) -> Result<Self, Error> {
        let window_h3_resolution = determinate_window_h3_resolution(
            &options.tableset,
            options.target_h3_resolution,
            options.window_max_size,
        );
        let window_indexes = build_window_indexes(&options.window_polygon, window_h3_resolution)?;

        // use a higher capacity to have a few available in case the consumer
        // of the sliding window sometimes discards single windows
        let output_capacity = max(3, options.concurrency as usize * 3);

        let (tx_output, rx_output) = tokio::sync::mpsc::channel(output_capacity);

        let shutdown = Arc::new(tokio::sync::Notify::new());
        let shutdown2 = shutdown.clone();

        let join_handle = tokio::task::spawn(async move {
            let shutdown_notified = shutdown2.notified();
            let window_iteration =
                launch_window_iteration(pool, tx_output, options, window_indexes);

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
            rx_output,
            join_handle: Some(join_handle),
            shutdown,
        })
    }

    pub async fn recv(&mut self) -> Option<Result<QueryOutput<ColumnSet>, Error>> {
        self.rx_output.recv().await
    }

    /// receive with a timeout
    ///
    /// return a tuple of the received data and a boolean indicating if the
    /// recv reached the timeout. (true -> was timeout, false -> no timeout)
    pub async fn recv_with_timeout(
        &mut self,
        duration: Duration,
    ) -> (Option<Result<QueryOutput<ColumnSet>, Error>>, bool) {
        match tokio::time::timeout(duration, self.recv()).await {
            Ok(received) => (received, false),
            Err(_elapsed) => {
                // timeout
                (None, true)
            }
        }
    }

    pub async fn shutdown(&mut self) -> Result<(), Error> {
        self.shutdown.notify_waiters();

        // let all tasks collapse
        self.rx_output.close();

        if let Some(handle) = self.join_handle.take() {
            handle.await??;
        };
        Ok(())
    }
}

#[instrument(level = "debug", skip(pool, tx_output, options, window_indexes))]
async fn launch_window_iteration(
    pool: Arc<Pool>,
    tx_output: tokio::sync::mpsc::Sender<Result<QueryOutput<ColumnSet>, Error>>,
    options: SlidingWindowOptions,
    window_indexes: VecDeque<H3Cell>,
) -> Result<(), Error> {
    let options_arc = Arc::new(options);
    let (tx_window_index, rx_window_index) =
        async_channel::bounded(options_arc.concurrency as usize);

    let mut fetch_handles = vec![];
    for _ in 0..options_arc.concurrency {
        let client = pool.get_handle().await?;
        let rx_window_index_ = rx_window_index.clone();
        let tx_output_ = tx_output.clone();
        let opts = options_arc.clone();
        let handle = tokio::task::spawn(async move {
            // fetch next window
            fetch_window(client, opts, rx_window_index_, tx_output_).await
        });
        fetch_handles.push(handle);
    }
    // close this tasks copy of the channel to leave no open copies once the tasks have finished.
    std::mem::drop(tx_output);
    std::mem::drop(rx_window_index);

    let prefetch_handle = {
        let client = pool.get_handle().await?;
        let opts = options_arc.clone();
        tokio::task::spawn(async move {
            // check window indexes
            prefetch_window_indexes(client, window_indexes, opts, tx_window_index).await
        })
    };

    prefetch_handle.await??;
    for handle in fetch_handles.drain(..) {
        handle.await??;
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
        warn!(
            "sliding window: using H3 res {} as window resolution to iterate over H3 res {} data. This is probably inefficient - try to increase window_max_size.",
            window_h3_resolution,
            target_h3_resolution
        );
    } else {
        info!(
            "sliding window: using H3 res {} as window resolution",
            window_h3_resolution
        );
    }
    window_h3_resolution
}

fn build_window_indexes(
    poly: &Polygon<f64>,
    window_h3_resolution: u8,
) -> Result<VecDeque<H3Cell>, Error> {
    let mut window_index_set = HashSet::new();

    for h3index in polyfill(&poly, window_h3_resolution) {
        let index = H3Cell::try_from(h3index)?;
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
        let index = H3Cell::from_coordinate(&point.0, window_h3_resolution)?;
        window_index_set.insert(index);
    }
    info!(
        "sliding window: {} window indexes found",
        window_index_set.len()
    );

    let mut window_indexes: Vec<_> = window_index_set.drain().collect();

    // always process windows in the same order. This is probably easier for to
    // user when inspecting the results produced during the processing
    window_indexes.sort_unstable_by(cmp_index_by_coordinate);

    Ok(window_indexes.drain(..).collect())
}

/// prefetch until some data-containing indexes where found, or the
/// window has been completely crawled
async fn prefetch_window_indexes(
    mut client: ClientHandle,
    mut window_indexes: VecDeque<H3Cell>,
    options: Arc<SlidingWindowOptions>,
    tx_window_index: async_channel::Sender<H3Cell>,
) -> Result<(), Error> {
    set_clickhouse_max_threads(&mut client, options.window_num_clickhouse_threads).await?;

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
        let q = {
            let q = options.tableset.build_select_query(
                &h3indexes,
                match &options.prefetch_query {
                    Some(pq) => pq,
                    None => &options.query,
                },
            )?;
            format!("select distinct {} from ({})", COL_NAME_H3INDEX, q)
        };

        let window_h3indexes = {
            let n_h3indexes = h3indexes.len();
            let columnset =
                query_all_with_uncompacting(&mut client, q, h3indexes.drain(..).collect())
                    .instrument(span!(
                        Level::DEBUG,
                        "checking window indexes for data availability",
                        n_h3indexes
                    ))
                    .await?;
            window_indexes_from_columnset(columnset)?
        };

        match window_h3indexes {
            Some(h3indexes) => {
                for h3index in h3indexes.iter() {
                    if tx_window_index.send(H3Cell::new(*h3index)).await.is_err() {
                        debug!("receivers for window indexes are gone");
                        return Ok(());
                    }
                }
            }
            None => continue,
        }
    }
}

fn window_indexes_from_columnset(mut columnset: ColumnSet) -> Result<Option<Vec<u64>>, Error> {
    if let Some(colvec) = columnset.columns.remove(COL_NAME_H3INDEX) {
        if colvec.is_empty() {
            return Ok(None);
        }
        match colvec {
            ColVec::U64(mut h3indexes) => {
                // make the ordering more deterministic by sorting, deduplicate for safety in case
                // the prefetch query returns duplicates.
                h3indexes.sort_unstable_by(cmp_h3index_by_coordinate);
                h3indexes.dedup();

                Ok(Some(h3indexes))
            }
            _ => {
                error!(
                    "expected the '{}' column of the prefetch query to be UInt64",
                    COL_NAME_H3INDEX
                );
                Err(Error::IncompatibleDatatype)
            }
        }
    } else {
        error!(
            "expected the generated prefetch query to contain a column called '{}'",
            COL_NAME_H3INDEX
        );
        Err(Error::ColumnNotFound(COL_NAME_H3INDEX.to_string()))
    }
}

async fn fetch_window(
    mut client: ClientHandle,
    options: Arc<SlidingWindowOptions>,
    rx_window_index: async_channel::Receiver<H3Cell>,
    tx_output: tokio::sync::mpsc::Sender<Result<QueryOutput<ColumnSet>, Error>>,
) -> Result<(), Error> {
    set_clickhouse_max_threads(&mut client, options.window_num_clickhouse_threads).await?;

    loop {
        let window_index = match rx_window_index.recv().await {
            Ok(wi) => wi,
            Err(_) => {
                debug!("sender for window index dropped");
                break;
            }
        };

        if tx_output.is_closed() {
            break;
        }

        debug!("fetching data for window {}", window_index.to_string());
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

        if tx_output.is_closed() {
            break;
        }

        if child_indexes.is_empty() {
            debug!(
                "window {} without intersecting h3indexes skipped",
                window_index.to_string()
            );
            continue;
        }

        let query_string = options
            .tableset
            .build_select_query(&child_indexes, &options.query)?;

        let t_start = Instant::now();
        let output = query_all_with_uncompacting(
            &mut client,
            query_string,
            child_indexes.iter().cloned().collect(),
        )
        .instrument(span!(
            Level::DEBUG,
            "Loading window data from DB",
            window_index = window_index.to_string().as_str()
        ))
        .await
        .map(|columnset| QueryOutput {
            data: columnset,
            h3indexes_queried: Some(child_indexes),
            window_h3index: Some(window_index.h3index()),
            query_duration: Some(t_start.elapsed()),
        });

        if tx_output.send(output).await.is_err() {
            debug!("Receiver for window resultset dropped");
            return Ok(());
        }
    }
    Ok(())
}

fn cmp_h3index_by_coordinate(h1: &u64, h2: &u64) -> Ordering {
    let cell1 = H3Cell::new(*h1);
    let cell2 = H3Cell::new(*h2);
    cmp_index_by_coordinate(&cell1, &cell2)
}

fn cmp_index_by_coordinate(cell1: &H3Cell, cell2: &H3Cell) -> Ordering {
    let coord1 = cell1.to_coordinate();
    let coord2 = cell2.to_coordinate();
    cmp_coordinate(&coord1, &coord2)
}

/// sort by north->south, west->east location
fn cmp_coordinate(coord1: &Coordinate<f64>, coord2: &Coordinate<f64>) -> Ordering {
    if (coord1.x - coord2.x).abs() < f64::EPSILON && (coord1.y - coord2.y).abs() < f64::EPSILON {
        Ordering::Equal
    } else if coord1.y > coord2.y {
        Ordering::Less
    } else if coord1.y < coord2.y {
        Ordering::Greater
    } else {
        coord2.x.partial_cmp(&coord1.y).unwrap_or(Ordering::Equal)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use geo::Coordinate;
    use h3ron::H3Cell;

    use crate::clickhouse::compacted_tables::{TableSet, TableSpec};
    use crate::clickhouse::window::{cmp_index_by_coordinate, window_index_resolution, cmp_coordinate};

    fn some_tableset() -> TableSet {
        TableSet {
            basename: "t1".to_string(),
            base_tables: {
                let mut hs = HashMap::new();
                for r in 0..=6 {
                    hs.insert(
                        r,
                        TableSpec {
                            h3_resolution: r,
                            is_compacted: false,
                            temporary_key: None,
                            has_base_suffix: true,
                        },
                    );
                }
                hs
            },
            compacted_tables: Default::default(),
            columns: Default::default(),
        }
    }

    #[test]
    fn test_window_index_resolution() {
        let ts = some_tableset();
        assert_eq!(window_index_resolution(&ts, 6, 1000), 3);
    }

    #[test]
    fn test_cmp_index_by_coordinate_vec() {
        let c1 = H3Cell::from_coordinate(&Coordinate::from((10.0, 20.0)), 6).unwrap();
        let c2 = H3Cell::from_coordinate(&Coordinate::from((20.0, 10.0)), 6).unwrap();
        let mut v = vec![c1.clone(), c2.clone()];
        v.sort_unstable_by(cmp_index_by_coordinate);
        assert_eq!(v[0], c1);
        assert_eq!(v[1], c2);
    }

    #[test]
    fn test_sort_by_coordinate() {
        let c1 = Coordinate::from((10.0, 20.0));
        let c2 = Coordinate::from((20.0, 10.0));
        let c3 = Coordinate::from((20.0, -20.0));
        let c4 = Coordinate::from((20.0, 8.0));
        let mut v = vec![c1.clone(), c2.clone(), c3.clone(), c4.clone()];
        v.sort_unstable_by(cmp_coordinate);
        assert_eq!(v[0], c1);
        assert_eq!(v[1], c2);
        assert_eq!(v[2], c4);
        assert_eq!(v[3], c3);
    }
}

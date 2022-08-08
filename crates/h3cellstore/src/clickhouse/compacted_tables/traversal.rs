use arrow_h3::algo::ToIndexCollection;
use arrow_h3::export::h3ron::iter::change_resolution;
use arrow_h3::export::h3ron::{H3Cell, ToH3Cells};
use arrow_h3::H3DataFrame;
use clickhouse_arrow_grpc::export::tonic::transport::Channel;
use clickhouse_arrow_grpc::ClickHouseClient;
use futures::Stream;
use geo_types::Geometry;
use postage::prelude::{Sink, Stream as _};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::spawn;
use tokio::task::spawn_blocking;
use tracing::{debug, debug_span, info, Instrument};

use crate::clickhouse::compacted_tables::{
    CompactedTablesStore, QueryOptions, TableSet, TableSetQuery,
};
use crate::Error;

/// find the resolution generate coarser h3 cells to access the tableset without needing to fetch more
/// than `max_h3indexes_fetch_count` indexes per batch.
///
/// That resolution must be a base resolution
fn select_traversal_resolution(
    tableset: &TableSet,
    target_h3_resolution: u8,
    max_h3indexes_fetch_count: usize,
) -> u8 {
    let mut resolutions: Vec<_> = tableset
        .base_resolutions()
        .iter()
        .filter(|r| **r < target_h3_resolution)
        .copied()
        .collect();
    resolutions.sort_unstable();

    let mut traversal_resolution = target_h3_resolution;
    for r in resolutions {
        let r_diff = (target_h3_resolution - r) as u32;
        if 7_u64.pow(r_diff) <= (max_h3indexes_fetch_count as u64) {
            traversal_resolution = r;
            break;
        }
    }
    info!(
        "traversal: using H3 res {} as traversal_resolution",
        traversal_resolution
    );
    traversal_resolution
}

pub struct TraversalOptions {
    /// the query to run
    pub query: TableSetQuery,

    /// the h3 resolutions which shall be fetched
    pub h3_resolution: u8,

    /// The maximum number of cells to fetch in one DB query.
    ///
    /// Please note that this setting controls only the number of cells
    /// requested from the DB. Should - for example - each cell have data
    /// for multiple time steps in the database, more rows will be returned.
    ///
    /// This setting is crucial to control the size of the messages transferred from
    /// Clickhouse. So, decrease when Clickhouse runs into GRPC message size limits
    /// (protobuf supports max. 2GB).
    pub max_h3indexes_fetch_count: usize,

    /// Number of parallel DB connections to use in the background.
    /// Depending with the number of connections used the amount of memory used increases as well as
    /// the load put onto the DB-Server. The benefit is getting data faster as it is pre-loaded in the
    /// background.
    pub num_connections: usize,

    /// optional prefilter query.
    ///
    /// This query will be applied to the tables in the reduced `traversal_h3_resolution` and only cells
    /// found by this query will be loaded from the tables in the requested full resolution
    pub filter_query: Option<TableSetQuery>,

    /// uncompact the cells loaded from the db. This should be true in most cases.
    pub do_uncompact: bool,
}

impl Default for TraversalOptions {
    fn default() -> Self {
        Self {
            query: TableSetQuery::AutoGenerated,
            h3_resolution: 0,
            max_h3indexes_fetch_count: 500,
            num_connections: 3,
            filter_query: None,
            do_uncompact: true,
        }
    }
}

impl TraversalOptions {
    pub fn with_h3_resolution(h3_resolution: u8) -> Self {
        Self {
            h3_resolution,
            ..Default::default()
        }
    }

    pub fn with_query_and_h3_resolution(query: TableSetQuery, h3_resolution: u8) -> Self {
        Self {
            query,
            h3_resolution,
            ..Default::default()
        }
    }
}

pub enum TraversalArea {
    Geometry(Geometry<f64>),
    H3Cells(Vec<H3Cell>),
}

impl TraversalArea {
    ///
    ///
    /// The cells are returned sorted for a deterministic traversal order
    pub fn to_cells(&self, traversal_resolution: u8) -> Result<Vec<H3Cell>, Error> {
        let mut cells = match self {
            TraversalArea::Geometry(geometry) => {
                let mut cells: Vec<_> =
                    geometry.to_h3_cells(traversal_resolution)?.iter().collect();

                // always add the outer vertices of polygons to ensure having always cells
                // even when the polygon is too small to have any cells inside
                match geometry {
                    Geometry::Polygon(poly) => {
                        cells.extend(poly.exterior().to_h3_cells(traversal_resolution)?.iter())
                    }
                    Geometry::MultiPolygon(mpoly) => {
                        for poly in mpoly.0.iter() {
                            cells.extend(poly.exterior().to_h3_cells(traversal_resolution)?.iter());
                        }
                    }
                    _ => (),
                };
                cells
            }
            TraversalArea::H3Cells(cells) => {
                let cells: Vec<_> = change_resolution(cells.as_slice(), traversal_resolution)
                    .collect::<Result<Vec<_>, _>>()?;
                cells
            }
        };

        cells.sort_unstable();
        cells.dedup();
        cells.shrink_to_fit();
        Ok(cells)
    }
}

impl From<Geometry<f64>> for TraversalArea {
    fn from(geom: Geometry<f64>) -> Self {
        Self::Geometry(geom)
    }
}

impl From<Vec<H3Cell>> for TraversalArea {
    fn from(cells: Vec<H3Cell>) -> Self {
        Self::H3Cells(cells)
    }
}

pub struct Traverser {
    pub num_traversal_cells: usize,
    pub traversal_h3_resolution: u8,
    dataframe_recv: tokio::sync::mpsc::Receiver<Result<TraversedCell, Error>>,
    num_cells_already_traversed: usize,
}

impl Stream for Traverser {
    type Item = Result<TraversedCell, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let self_mut = self.get_mut();
        let polled = self_mut.dataframe_recv.poll_recv(cx);

        if polled.is_ready() {
            self_mut.num_cells_already_traversed += 1;
        }
        polled
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // yielding less elements than hinted is allowed, though not best practice.
        // We may yield less here when traversal cells to not contain data
        let num_cells_outstanding = self
            .num_traversal_cells
            .saturating_sub(self.num_cells_already_traversed);
        (num_cells_outstanding, Some(num_cells_outstanding))
    }
}

pub async fn traverse(
    client: &mut ClickHouseClient<Channel>,
    database_name: String,
    tableset_name: String,
    area: &TraversalArea,
    options: TraversalOptions,
) -> Result<Traverser, Error> {
    let tableset = client.get_tableset(&database_name, tableset_name).await?;
    let traversal_h3_resolution = select_traversal_resolution(
        &tableset,
        options.h3_resolution,
        options.max_h3indexes_fetch_count,
    );

    let traversal_cells = area.to_cells(traversal_h3_resolution)?;

    traverse_inner(
        client,
        database_name,
        tableset,
        traversal_cells,
        options,
        traversal_h3_resolution,
    )
    .await
}

async fn traverse_inner(
    client: &mut ClickHouseClient<Channel>,
    database_name: String,
    tableset: TableSet,
    traversal_cells: Vec<H3Cell>,
    options: TraversalOptions,
    traversal_h3_resolution: u8,
) -> Result<Traverser, Error> {
    let do_uncompact = options.do_uncompact;
    let num_traversal_cells = traversal_cells.len();
    let h3_resolution = options.h3_resolution;
    let mut context = WorkerContext {
        client: client.clone(),
        database_name,
        tableset,
    };
    let (dataframe_send, dataframe_recv) = tokio::sync::mpsc::channel(options.num_connections);

    let _background_fetch = spawn(async move {
        let (mut trav_cells_send, _trav_cells_recv) =
            postage::dispatch::channel(2 * options.num_connections);

        // spawn the workers performing the db-work
        for _ in 0..(options.num_connections) {
            let mut worker_context = context.clone();
            let mut worker_trav_cells_recv = trav_cells_send.subscribe();
            let worker_dataframe_send = dataframe_send.clone();
            let worker_query = options.query.clone();

            spawn(async move {
                while let Some(cell) = worker_trav_cells_recv.recv().await {
                    let message = match load_traversed_cell(
                        &mut worker_context,
                        worker_query.clone(),
                        cell,
                        h3_resolution,
                        do_uncompact,
                    )
                    .await
                    {
                        Ok(Some(traversed_cell)) => Ok(traversed_cell),
                        Ok(None) => {
                            // no data found, continue to the next cell
                            info!("traversal cell yielded no data - skipping");
                            continue;
                        }
                        Err(e) => Err(e),
                    };

                    if worker_dataframe_send.send(message).await.is_err() {
                        debug!("worker channel has been closed upstream. shutting down worker");
                        break;
                    } else {
                        info!("traversal cell loaded and send");
                    }
                }
            });
        }

        // distribute the cells to the workers
        let _ = spawn(async move {
            if let Some(filter_query) = &options.filter_query {
                for cell_chunk in traversal_cells.chunks(50) {
                    dispatch_traversal_cells(
                        &mut trav_cells_send,
                        prefilter_traversal_cells(
                            &mut context,
                            filter_query.clone(),
                            cell_chunk,
                            traversal_h3_resolution,
                        )
                        .await,
                    )
                    .await;
                }
            } else {
                dispatch_traversal_cells(&mut trav_cells_send, Ok(traversal_cells)).await;
            }
        });
    });

    // end of this scope closes the local copy of the dataframe_send channel to allow the
    // pipeline to collapse when ta traversal is finished

    Ok(Traverser {
        num_traversal_cells,
        traversal_h3_resolution,
        dataframe_recv,
        num_cells_already_traversed: 0,
    })
}

#[derive(Clone)]
struct WorkerContext {
    client: ClickHouseClient<Channel>,
    database_name: String,
    tableset: TableSet,
}

async fn dispatch_traversal_cells(
    sender: &mut postage::dispatch::Sender<Result<H3Cell, Error>>,
    traversal_cells: Result<Vec<H3Cell>, Error>,
) {
    match traversal_cells {
        Ok(cells) => {
            for cell in cells {
                if sender.send(Ok(cell)).await.is_err() {
                    debug!("sink rejected message");
                    break;
                }
            }
        }
        Err(e) => {
            if sender.send(Err(e)).await.is_err() {
                debug!("sink rejected message");
            }
        }
    }
}

async fn prefilter_traversal_cells(
    worker_context: &mut WorkerContext,
    filter_query: TableSetQuery,
    cells: &[H3Cell],
    traversal_h3_resolution: u8,
) -> Result<Vec<H3Cell>, Error> {
    if cells.is_empty() {
        return Ok(vec![]);
    }

    let filter_h3df = worker_context
        .client
        .query_tableset_cells(
            &worker_context.database_name,
            worker_context.tableset.clone(),
            QueryOptions::new(filter_query, cells.to_vec(), traversal_h3_resolution),
        )
        .await?;

    // use only the indexes from the filter query to be able to fetch a smaller subset
    Ok(spawn_blocking(move || {
        filter_h3df
            .to_index_collection()
            .map(|mut cells: Vec<H3Cell>| {
                // remove duplicates
                cells.sort_unstable();
                cells.dedup();
                cells
            })
    })
    .await??)
}

pub struct TraversedCell {
    /// the traversal cell whose child cells where loaded
    pub cell: H3Cell,

    /// dataframe containing the data of the child cells
    pub contained_data: H3DataFrame,
}

async fn load_traversed_cell(
    worker_context: &mut WorkerContext,
    query: TableSetQuery,
    cell: Result<H3Cell, Error>,
    h3_resolution: u8,
    do_uncompact: bool,
) -> Result<Option<TraversedCell>, Error> {
    match cell {
        Ok(cell) => {
            let mut query_options = QueryOptions::new(query, vec![cell], h3_resolution);
            query_options.do_uncompact = do_uncompact;

            let contained_data = worker_context
                .client
                .query_tableset_cells(
                    &worker_context.database_name,
                    worker_context.tableset.clone(),
                    query_options,
                )
                .instrument(debug_span!(
                    "Loading traversal cell",
                    cell = cell.to_string().as_str()
                ))
                .await?;

            if contained_data.dataframe.shape().0 == 0 {
                // no data found, continue to the next cell
                info!("Discarding received empty dataframe");
                return Ok(None);
            }
            Ok(Some(TraversedCell {
                cell,
                contained_data,
            }))
        }
        Err(e) => Err(e),
    }
}

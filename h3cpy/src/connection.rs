use std::collections::HashSet;

use clickhouse_rs::{
    ClientHandle,
    Pool,
};
use geo::algorithm::intersects::Intersects;
use h3ron::index::Index;
use numpy::{IntoPyArray, Ix1, PyArray, PyReadonlyArray1};
use pyo3::{
    prelude::*,
    Py,
    PyResult,
    Python,
    exceptions::PyRuntimeError,
};
use tokio::{
    runtime::Runtime,
    task,
};
use futures_util::StreamExt;

use h3cpy_int::{
    compacted_tables::{
        TableSet,
        find_tablesets,
    },
    window::WindowFilter,
};

use crate::{
    geometry::polygon_from_python,
    inspect::TableSet as TableSetWrapper,
    window::{
        create_window,
        SlidingH3Window,
    },
};

pub(crate) struct RuntimedPool {
    pub(crate) pool: Pool,
    pub(crate) rt: Runtime,
}

impl RuntimedPool {
    pub fn create(db_url: &str) -> PyResult<RuntimedPool> {
        let rt = match Runtime::new() {
            Ok(rt) => rt,
            Err(e) => return Err(PyRuntimeError::new_err(format!("could not create tokio rt: {:?}", e)))
        };
        Ok(Self {
            pool: Pool::new(db_url),
            rt,
        })
    }

    pub fn get_client(&mut self) -> PyResult<ClientHandle> {
        let p = &self.pool;
        match self.rt.block_on(async { p.get_handle().await }) {
            Ok(client) => Ok(client),
            Err(e) => Err(PyRuntimeError::new_err(format!("could not create clickhouse client: {:?}", e)))
        }
    }
}

async fn list_tablesets(mut ch: ClientHandle) -> PyResult<Vec<TableSetWrapper>> {
    let mut stream = ch.query("select table
                from system.columns
                where name = 'h3index' and database = currentDatabase()"
    ).stream();

    let mut tablenames = vec![];
    while let Some(row_res) = stream.next().await {
        let row = row_res.map_err(|e| PyRuntimeError::new_err("no row"))?;
        let tablename: String = row.get("table").map_err(|e| PyRuntimeError::new_err(format!("{:?}", e)))?;
        tablenames.push(tablename);

    }
    Ok(find_tablesets(&tablenames)
        .drain()
        .map(|(k,v)| TableSetWrapper { inner: v})
        .collect())
}

#[pyclass]
pub struct ClickhouseConnection {
    pub(crate) rp: RuntimedPool,
}

#[pymethods]
impl ClickhouseConnection {
    /// proof-of-concept for numpy integration. using u64 as this will be the type for h3 indexes
    /// TODO: remove later
    pub fn poc_some_h3indexes(&self) -> PyResult<Py<PyArray<u64, Ix1>>> {
        let idx: Index = 0x89283080ddbffff_u64.into();
        let v: Vec<_> = idx.k_ring(80).iter().map(|i| i.h3index()).collect();
        let gil = Python::acquire_gil();
        let py = gil.python();
        Ok(v.into_pyarray(py).to_owned())
    }

    pub fn make_sliding_window(&self, window_poly_like: &PyAny, target_h3_resolution: u8, window_max_size: u32 /*, tableset, window_size, query*/) -> PyResult<SlidingH3Window> {
        let window_polygon = polygon_from_python(window_poly_like)?;

        // iterators in pyo3: https://github.com/PyO3/pyo3/issues/1085#issuecomment-670835739
        // TODO
        let ts = TableSet {
            basename: "t1".to_string(),
            base_h3_resolutions: {
                let mut hs = HashSet::new();
                for r in 0..=6 {
                    hs.insert(r);
                }
                hs
            },
            compacted_h3_resolutions: Default::default(),
        };
        create_window(window_polygon, &ts, target_h3_resolution, window_max_size)
    }

    fn list_tablesets(&mut self) -> PyResult<Vec<TableSetWrapper>> {
        let client = self.rp.get_client()?;
        self.rp.rt.block_on(async {
            list_tablesets(client).await
        })
    }

    fn fetch_tableset(&self, tableset: &TableSetWrapper, h3indexes: PyReadonlyArray1<u64>) -> PyResult<ResultSet> {
        Ok(ResultSet { columns: Default::default() }) // TODO
    }

    fn has_data(&self, tableset: &TableSetWrapper, h3index: u64) -> bool {
        true // TOOO
    }


    pub fn fetch_next_window(&self, py: Python<'_>, tableset: &TableSetWrapper, sliding_h3_window: &mut SlidingH3Window) -> PyResult<Option<ResultSet>> {
        while let Some(window_h3index) = sliding_h3_window.next_window() {
            // check if the window index contains any data on coarse resolution, when not,
            // then there is no need to load anything
            if !self.has_data(tableset, window_h3index) {
                continue;
            }

            let child_indexes: Vec<_> = Index::from(window_h3index)
                .get_children(sliding_h3_window.target_h3_resolution)
                .drain(..)
                // remove children located outside the window_polygon. It is probably is not worth the effort,
                // but it allows to relocate some load to the client.
                .filter(|ci| {
                    let p = ci.polygon();
                    sliding_h3_window.window_rect.intersects(&p) && sliding_h3_window.window_polygon.intersects(&p)
                })
                .map(|i| i.h3index())
                .collect();
            return Ok(Some(self.fetch_tableset(tableset, child_indexes.into_pyarray(py).readonly())?));
        }
        Ok(None)
    }
}


/// filters indexes to only return those containing any data
/// in the clickhouse tableset
struct TableSetContainsDataFilter<'a> {
    tableset: &'a TableSet,
    connection: &'a ClickhouseConnection,
}

impl<'a> TableSetContainsDataFilter<'a> {
    pub fn new(connection: &'a ClickhouseConnection, tableset: &'a TableSet) -> Self {
        TableSetContainsDataFilter {
            tableset,
            connection,
        }
    }
}

impl<'a> WindowFilter for TableSetContainsDataFilter<'a> {
    fn filter(&self, window_index: &Index) -> bool {
        //unimplemented!()
        true
    }
}


#[pyclass]
pub struct ResultSet {
    columns: HashSet<String, u8>
}
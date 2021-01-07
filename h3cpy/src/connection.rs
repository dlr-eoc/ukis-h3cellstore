use std::collections::{HashSet, HashMap};

use clickhouse_rs::{
    ClientHandle,
    errors::Error as ChError,
    errors::Result as ChResult,
    Pool,
};
use futures_util::StreamExt;
use geo::algorithm::intersects::Intersects;
use h3ron::index::Index;
use log::warn;
use numpy::{IntoPyArray, Ix1, PyArray, PyReadonlyArray1};
use pyo3::{
    exceptions::PyRuntimeError,
    prelude::*,
    Py,
    PyResult,
    Python,
};
use tokio::{
    runtime::Runtime,
    task,
};

use h3cpy_int::{
    compacted_tables::{
        find_tablesets,
        TableSet,
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
        ch_to_pyresult(self.rt.block_on(async { p.get_handle().await }))
    }
}

async fn list_tablesets(mut ch: ClientHandle) -> ChResult<HashMap<String, TableSetWrapper>> {
    let mut tablesets = {
        let mut stream = ch.query("select table
                from system.columns
                where name = 'h3index' and database = currentDatabase()"
        ).stream();

        let mut tablenames = vec![];
        while let Some(row_res) = stream.next().await {
            let row = row_res?;
            let tablename: String = row.get("table")?;
            tablenames.push(tablename);
        }
        find_tablesets(&tablenames)
    };

    // find the columns for the tablesets
    for (ts_name, ts) in tablesets.iter_mut() {
        let set_table_names = itertools::join(
            ts.tables()
                .iter()
                .map(|t| format!("'{}'", t.to_table_name()))
            , ", ");

        let mut columns_stream = ch.query(format!("
            select name, type, count(*) as c
                from system.columns
                where table in ({})
                and database = currentDatabase()
                and not startsWith(name, 'h3index')
                group by name, type
        ", set_table_names)).stream();
        while let Some(c_row_res) = columns_stream.next().await {
            let c_row = c_row_res?;
            let c: u64 = c_row.get("c")?;
            let col_name: String = c_row.get("name")?;

            // column must be present in all tables of the set, or it is not usable
            if c as usize == ts.num_tables() {
                let col_type: String = c_row.get("type")?;
                ts.columns.insert(col_name, col_type);
            } else {
                warn!("column {} is not present using the same type in all tables of set {}. ignoring the column", col_name, ts_name);
            }
        }
    }

    Ok(tablesets
        .drain()
        .map(|(k, v)| (k, TableSetWrapper { inner: v }))
        .collect())
}

fn ch_to_pyerr(ch_err: ChError) -> PyErr {
    PyRuntimeError::new_err(format!("clickhouse error: {:?}", ch_err))
}

fn ch_to_pyresult<T>(res: ChResult<T>) -> PyResult<T> {
    match res {
        Ok(v) => Ok(v),
        Err(e) => Err(ch_to_pyerr(e))
    }
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

    pub fn make_sliding_window(&self, window_poly_like: &PyAny, tableset: &TableSetWrapper, target_h3_resolution: u8, window_max_size: u32) -> PyResult<SlidingH3Window> {
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
            columns: Default::default(),
        };
        create_window(window_polygon, &ts, target_h3_resolution, window_max_size)
    }


    fn list_tablesets(&mut self) -> PyResult<HashMap<String, TableSetWrapper>> {
        let client = self.rp.get_client()?;
        ch_to_pyresult(self.rp.rt.block_on(async {
            list_tablesets(client).await
        }))
    }

    fn fetch_tableset(&self, tableset: &TableSetWrapper, h3indexes: PyReadonlyArray1<u64>) -> PyResult<ResultSet> {
        Ok(ResultSet { columns: Default::default() }) // TODO
    }

    fn has_data(&self, tableset: &TableSetWrapper, h3index: u64) -> bool {
        true // TOOO
    }


    pub fn fetch_next_window(&self, py: Python<'_>, sliding_h3_window: &mut SlidingH3Window, tableset: &TableSetWrapper) -> PyResult<Option<ResultSet>> {
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
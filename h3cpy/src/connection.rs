use std::collections::HashSet;

use h3::index::Index;
use numpy::{IntoPyArray, Ix1, PyArray};
use pyo3::{
    prelude::*,
    Py,
    PyResult,
    Python,
};

use h3cpy_int::{
    compacted_tables::TableSet,
    window::WindowFilter,
};
use h3cpy_int::window::WindowIterator;

use crate::geometry::polygon_from_python;
use crate::window::SlidingH3Window;
use geo::algorithm::intersects::Intersects;

#[pyclass]
#[derive(Clone)]
pub struct ClickhouseConnection {}

#[pymethods]
impl ClickhouseConnection {
    #[new]
    pub fn new() -> Self {
        Self {}
    }

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
        SlidingH3Window::new(window_polygon, &ts, target_h3_resolution, window_max_size)
    }

    fn fetch_tableset(&self, tableset: &TableSet, h3indexes: &[u64]) {
        // TODO
    }

    fn has_data(&self, tableset: &TableSet, h3index: u64) -> bool {
        true // TOOO
    }


    pub fn fetch_next_window(&self, tableset: &TableSet, sliding_h3_window: &mut SlidingH3Window) {

        while let Some(window_index) = sliding_h3_window.next_window() {
            // check if the window index contains any data on coarse resolution, when not,
            // then there is no need to load anything
            if ! self.has_data(tableset, window_index.h3index()) {
                continue
            }

            let child_indexes: Vec<_> = window_index.get_children(sliding_h3_window.target_h3_resolution)
                    .drain(..)
                    // remove children located outside the window_polygon. It is probably is not worth the effort,
                    // but it allows to relocate some load to the client.
                    .filter(|ci| {
                        let p = ci.polygon();
                        sliding_h3_window.window_rect.intersects(&p) && sliding_h3_window.window_polygon.intersects(&p)
                    })
                    .map(|i| i.h3index())
                    .collect();
            return self.fetch_tableset(tableset, &child_indexes);
        }
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
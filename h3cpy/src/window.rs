use std::collections::HashSet;

use geo::algorithm::{
    bounding_rect::BoundingRect,
    centroid::Centroid,
};
use geo_types::{Polygon, Rect};
use h3ron::{Index, polyfill};
use h3ron_h3_sys::H3Index;
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    PyResult,
};

use h3cpy_int::{
    compacted_tables::{
        TableSet,
        TableSetQuery,
    },
    window::window_index_resolution,
};

#[pyclass]
pub struct SlidingH3Window {
    pub window_polygon: Polygon<f64>,
    pub window_rect: Rect<f64>,
    pub target_h3_resolution: u8,
    window_indexes: Vec<Index>,
    iter_pos: usize,
    pub(crate) query: TableSetQuery,
}

#[pymethods]
impl SlidingH3Window {
    pub fn next_window(&mut self) -> Option<H3Index> {
        if let Some(window_index) = self.window_indexes.get(self.iter_pos) {
            self.iter_pos += 1;
            Some(window_index.h3index())
        } else {
            None
        }
    }
}

pub fn create_window(window_polygon: Polygon<f64>, table_set: &TableSet, target_h3_resolution: u8, window_max_size: u32, query: TableSetQuery) -> PyResult<SlidingH3Window> {
    let window_rect = match window_polygon.bounding_rect() {
        Some(w) => w,
        None => return Err(PyValueError::new_err("empty polygon given"))
    };

    let window_res = window_index_resolution(table_set, target_h3_resolution, window_max_size);

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


    Ok(SlidingH3Window {
        window_polygon,
        window_rect,
        target_h3_resolution,
        window_indexes: window_index_set
            .drain()
            .collect(),
        iter_pos: 0,
        query,
    })
}


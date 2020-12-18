use std::collections::HashSet;
use std::iter::FromIterator;

use geo_types::{Polygon, Rect};
use geo::algorithm::{
    bounding_rect::BoundingRect,
    intersects::Intersects
};
use h3::index::Index;
use h3::polyfill;
use h3_sys::H3Index;

use h3cpy_int::compacted_tables::TableSet;
use h3cpy_int::window::window_index_resolution;
use pyo3::PyResult;
use pyo3::exceptions::PyValueError;
use std::ops::Try;


pub struct SlidingH3Window {
    pub window_polygon: Polygon<f64>,
    pub window_rect: Rect<f64>,
    pub target_h3_resolution: u8,
    window_indexes: Vec<Index>,
    iter_pos: usize
}

impl SlidingH3Window {
    pub fn new(window_polygon: Polygon<f64>, table_set: &TableSet, target_h3_resolution: u8, window_max_size: u32) -> PyResult<Self> {
        let window_rect = window_polygon.bounding_rect()
            .into_result()
            .map_err(|e| PyValueError::new_err("empty polygon given"))?;

        let window_res = window_index_resolution(table_set, target_h3_resolution, window_max_size);

        let mut window_index_set = HashSet::new();
        for h3index in polyfill(&window_polygon, window_res) {
            let index = Index::from(h3index);
            window_index_set.insert(index);

            // polyfill just uses the centroid to determinate if an index is convert,
            // but we also want intersecting h3 cells where the centroid may be outside
            // of the polygon, so we add the direct neighbors as well.
            for ring_h3index in Index::from(h3index).k_ring(1) {
                window_index_set.insert(ring_h3index);
            }
        }

        // window_h3index must really intersect with the window
        let window_indexes: Vec<_> = window_index_set
            .drain()
            .filter(|wi| window_polygon.intersects(&wi.polygon()))
            .collect();

        Ok(Self {
            window_polygon,
            window_rect,
            target_h3_resolution,
            window_indexes,
            iter_pos: 0
        })
    }

    pub fn next_window(&mut self) -> Option<Index> {
        if let Some(window_index) = self.window_indexes.get(self.iter_pos) {
            self.iter_pos += 1;
            Some(window_index.clone())
        } else {
            None
        }
    }
}
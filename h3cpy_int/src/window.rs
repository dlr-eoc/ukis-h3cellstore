use std::collections::HashSet;
use std::iter::FromIterator;

use geo::algorithm::intersects::Intersects;
use geo_types::Polygon;
use h3::{
    index::Index,
    polyfill,
};
use h3_sys::H3Index;

use crate::compacted_tables::TableSet;

/// find the resolution generate coarser h3-indexes to access the tableset without needing to fetch more
/// than window_max_size indexes per batch.
///
/// That resolution must be a base resolution
fn window_index_resolution(table_set: &TableSet, target_h3_resolution: u8, window_max_size: u32) -> u8 {
    let mut resolutions = Vec::from_iter(table_set.base_h3_resolutions
        .iter()
        .filter(|r| **r < target_h3_resolution)
        .map(|r| r.clone()));
    resolutions.sort();

    let mut window_h3_resolution = target_h3_resolution;
    for r in resolutions {
        if 7_u32.pow((target_h3_resolution - r) as u32) <= window_max_size {
            window_h3_resolution = r;
            break;
        }
    }
    window_h3_resolution
}

struct WindowInterator {
    window_polygon: Polygon<f64>,
    target_h3_resolution: u8,
    window_indexes: Vec<H3Index>,
    iter_pos: usize,
}

impl WindowInterator {
    pub fn new(window_polygon: Polygon<f64>, table_set: &TableSet, target_h3_resolution: u8, window_max_size: u32) -> Self {
        let window_res = window_index_resolution(table_set, target_h3_resolution, window_max_size);

        let mut window_index_set = HashSet::new();
        for h3index in polyfill(&window_polygon, window_res) {
            window_index_set.insert(h3index);

            // polyfill just uses the centroid to determinate if an index is convert,
            // but we also want intersecting h3 cells where the centroid may be outside
            // of the polygon, so we add the direct neighbors as well.
            for ring_h3index in Index::from(h3index).k_ring(1) {
                window_index_set.insert(ring_h3index.h3index());
            }
        }

        Self {
            window_polygon,
            target_h3_resolution,
            window_indexes: Vec::from_iter(window_index_set.drain()),
            iter_pos: 0,
        }
    }
}

struct Window {
    pub window_index: Index,
    pub indexes: Vec<Index>,
}

impl Iterator for WindowInterator {
    type Item = Window;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(h3index) = self.window_indexes.get(self.pos) {
                pos += 1;
                let window_index = Index::from(h3index);

                // window_h3index must really intersect with the window
                if !self.window_polygon.intersects(&window_index.polygon()) {
                    continue;
                }

                // TODO: allow pre-check if there is any data in the window

                return Some(
                    Window {
                        window_index,
                        indexes: window_index.get_children(self.target_h3_resolution),
                    }
                );
            } else {
                break;
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::iter::FromIterator;

    use crate::compacted_tables::TableSet;
    use crate::window::window_index_resolution;

    #[test]
    fn test_window_index_resolution() {
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

        assert_eq!(
            window_index_resolution(&ts, 6, 1000),
            3
        );
    }
}

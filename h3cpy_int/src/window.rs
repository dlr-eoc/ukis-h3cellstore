use std::collections::HashSet;

use geo::algorithm::bounding_rect::BoundingRect;
use geo::algorithm::intersects::Intersects;
use geo_types::Polygon;
use h3ron::{
    index::Index,
    polyfill,
};
use h3ron_h3_sys::H3Index;

use crate::compacted_tables::TableSet;

/// find the resolution generate coarser h3-indexes to access the tableset without needing to fetch more
/// than window_max_size indexes per batch.
///
/// That resolution must be a base resolution
pub fn window_index_resolution(table_set: &TableSet, target_h3_resolution: u8, window_max_size: u32) -> u8 {
    let mut resolutions: Vec<_> = table_set.base_h3_resolutions
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


pub trait WindowFilter {
    /// return true when the window should be used, return false when not
    fn filter(&self, window_index: &Index) -> bool;
}

pub struct WindowIterator<F: WindowFilter> {
    window_polygon: Polygon<f64>,
    target_h3_resolution: u8,
    window_indexes: Vec<H3Index>,
    iter_pos: usize,
    window_filter: F,
}

impl<F: WindowFilter> WindowIterator<F> {
    pub fn new(window_polygon: Polygon<f64>, table_set: &TableSet, target_h3_resolution: u8, window_max_size: u32, window_filter: F) -> Self {
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
            window_indexes: window_index_set.drain().collect(),
            iter_pos: 0,
            window_filter,
        }
    }
}

pub struct Window {
    pub window_index: Index,
    pub indexes: Vec<Index>,
}


impl<F: WindowFilter> Iterator for WindowIterator<F> {
    type Item = Window;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(h3index) = self.window_indexes.get(self.iter_pos) {
            self.iter_pos += 1;
            let window_index = Index::from(*h3index);

            // window_h3index must really intersect with the window
            if !self.window_polygon.intersects(&window_index.polygon()) {
                continue;
            }

            // apply the filter after the intersects, as the filter may be more
            // expensive to compute
            if !self.window_filter.filter(&window_index) {
                continue;
            }

            let child_indexes: Vec<_> = if let Some(window_rect) = self.window_polygon.bounding_rect() {
                window_index.get_children(self.target_h3_resolution)
                    .drain(..)
                    // remove children located outside the window_polygon. It is probably is not worth the effort,
                    // but it allows to relocate some load to the client.
                    .filter(|ci| {
                        let p = ci.polygon();
                        window_rect.intersects(&p) && self.window_polygon.intersects(&p)
                    })
                    .collect()
            } else {
                continue; // TODO: when is there no rect?
            };


            return Some(Window {
                indexes: child_indexes,
                window_index,
            });
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use geo_types::{LineString, Polygon};
    use h3ron::index::Index;

    use crate::compacted_tables::TableSet;
    use crate::window::{window_index_resolution, WindowFilter, WindowIterator};

    fn some_tableset() -> TableSet {
        TableSet {
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
        }
    }

    #[test]
    fn test_window_index_resolution() {
        let ts = some_tableset();
        assert_eq!(
            window_index_resolution(&ts, 6, 1000),
            3
        );
    }

    struct OddFilter {}

    impl WindowFilter for OddFilter {
        fn filter(&self, window_index: &Index) -> bool {
            (window_index.h3index() % 2) == 1
        }
    }

    #[test]
    fn test_window_iterator_filter() {
        let window = Polygon::new(
            LineString::from(vec![(40., 40.), (10., 10.), (10., 40.), (40., 40.)]),
            vec![],
        );
        let ts = some_tableset();

        let w_iter = WindowIterator::new(window, &ts, 6, 1000, OddFilter {});
        let mut n_windows = 0_usize;
        for window in w_iter {
            assert_eq!(window.window_index.h3index() % 2, 1);
            assert!(window.indexes.len() < 1000);
            assert!(window.indexes.len() > 0);
            n_windows += 1;
        }
        assert!(n_windows > 100);
    }
}

use geo::algorithm::convex_hull::ConvexHull;
use geo_types::{MultiPolygon, Polygon};
use h3ron::{H3Cell, ToPolygon};
use ndarray::{ArrayView, Ix1};

use crate::error::Error;

/// calculate the convex hull of an array og h3 indexes
pub fn h3indexes_convex_hull(h3indexes_arr: &ArrayView<u64, Ix1>) -> Result<Polygon<f64>, Error> {
    let mut polygons = Vec::with_capacity(h3indexes_arr.len());
    for h3index in h3indexes_arr.iter() {
        let index = H3Cell::try_from(*h3index)?;
        polygons.push(index.to_polygon());
    }
    Ok(MultiPolygon(polygons).convex_hull())
}

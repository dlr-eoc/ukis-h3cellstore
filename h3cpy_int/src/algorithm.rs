use geo::algorithm::convex_hull::ConvexHull;
use geo_types::{MultiPolygon, Polygon};
use h3ron::{Index, ToPolygon};
use ndarray::{ArrayView, Ix1};

/// calculate the convex hull of an array og h3 indexes
pub fn h3indexes_convex_hull(h3indexes_arr: &ArrayView<u64, Ix1>) -> Polygon<f64> {
    let mp = MultiPolygon(
        h3indexes_arr
            .iter()
            .map(|hi| Index::from(*hi).to_polygon())
            .collect(),
    );
    mp.convex_hull()
}

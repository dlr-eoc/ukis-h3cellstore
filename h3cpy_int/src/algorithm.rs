use geo_types::{Polygon, MultiPolygon};
use ndarray::ArrayView1;
use h3ron::{Index, ToPolygon};
use geo::algorithm::convex_hull::ConvexHull;

/// calculate the convex hull of an array og h3 indexes
pub fn h3indexes_convex_hull(h3indexes_arr: &ArrayView1<u64>) -> Polygon<f64> {
    let mp = MultiPolygon(h3indexes_arr.iter()
        .map(|hi| Index::from(*hi).to_polygon())
        .collect()
    );
    mp.convex_hull()
}
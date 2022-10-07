use crate::Error;
use cavalier_contours::polyline::{PlineCreation, PlineSource, PlineSourceMut, Polyline};
use geo::{BoundingRect, Densify, EuclideanLength, Winding};
use geo_types::{Line, LineString, Polygon};
use h3ron::collections::HashSet;
use h3ron::{H3Cell, ToCoordinate, ToPolygon};
use ordered_float::OrderedFloat;

/// find the cells located directly within the exterior ring of the given polygon
pub fn border_cells(poly: &Polygon, h3_resolution: u8) -> Result<HashSet<H3Cell>, Error> {
    let ext_ring = {
        let mut ext_ring = poly.exterior().clone();
        ext_ring.make_ccw_winding(); // make coord order deterministic so the offset direction is correct
        ext_ring
    };

    let cell_radius = max_cell_radius(&ext_ring, h3_resolution)?;
    let buffer_offset = cell_radius;

    let mut out_cells = HashSet::default();
    for ext_line_segment in ext_ring.0.windows(2) {
        let line = Line::new(ext_line_segment[0], ext_line_segment[1]);
        let line_rect = line.bounding_rect();

        let mut pline = Polyline::with_capacity(2, false);
        pline.add(line.start.x, line.start.y, 0.0);
        pline.add(line.end.x, line.end.y, 0.0);

        for offsetted in pline.parallel_offset(buffer_offset) {
            let l = offsetted.vertex_data.len();
            if l >= 2 {
                let ls = Line::new(
                    (offsetted.vertex_data[0].x, offsetted.vertex_data[0].y),
                    (
                        offsetted.vertex_data[l - 1].x,
                        offsetted.vertex_data[l - 1].y,
                    ),
                )
                .densify(cell_radius);

                for c in ls.0.iter() {
                    let cell = H3Cell::from_coordinate(*c, h3_resolution)?;

                    // reverse check as the cell may be beyond the start or endpoint of the line.
                    // bbox containment check is not applicable because of possible strictly vertical
                    // or horizontal line segments.
                    let cell_coord = cell.to_coordinate()?;
                    if !((cell_coord.x < line_rect.min().x && cell_coord.y < line_rect.min().y)
                        || (cell_coord.x > line_rect.max().x && cell_coord.y > line_rect.max().y)
                        || (cell_coord.x > line_rect.max().x && cell_coord.y < line_rect.min().y)
                        || (cell_coord.x < line_rect.min().x && cell_coord.y > line_rect.max().y))
                    {
                        out_cells.insert(cell);
                    }
                }
            }
        }
    }
    Ok(out_cells)
}

fn max_cell_radius(ls: &LineString, h3_resolution: u8) -> Result<f64, Error> {
    let lengths =
        ls.0.iter()
            .map(|c| {
                H3Cell::from_coordinate(*c, h3_resolution)
                    .map_err(Error::from)
                    .and_then(|cell| cell_radius(cell).map(OrderedFloat::from))
            })
            .collect::<Result<Vec<_>, _>>()?;
    Ok(*lengths
        .iter()
        .copied()
        .max()
        .unwrap_or_else(|| OrderedFloat::from(0.0)))
}

fn cell_radius(cell: H3Cell) -> Result<f64, Error> {
    let center = cell.to_coordinate()?;
    let poly = cell.to_polygon()?;
    Ok(Line::new(center, poly.exterior().0[0])
        .euclidean_length()
        .abs())
}

#[cfg(test)]
mod tests {
    use crate::geom::border_cells;
    use geo_types::{Geometry, GeometryCollection, Point, Rect};
    use h3ron::collections::HashSet;
    use h3ron::{ToCoordinate, ToH3Cells};

    #[test]
    fn border_cells_within_rect() {
        let rect = Rect::new((30.0, 30.0), (50.0, 50.0));
        let h3_resolution = 7;

        let filled_cells = HashSet::from_iter(rect.to_h3_cells(h3_resolution).unwrap().iter());
        let border = border_cells(&rect.to_polygon(), h3_resolution).unwrap();
        dbg!(filled_cells.len(), border.len());
        assert!(border.len() > 100);

        // write geojson for visual inspection
        /*
        let mut geoms: Vec<Geometry> = vec![rect.into()];
        for bc in border.iter() {
            geoms.push(Point::from(bc.to_coordinate().unwrap()).into());
        }
        let gc = GeometryCollection::from(geoms);
        let fc = geojson::FeatureCollection::from(&gc);
        std::fs::write("/tmp/border.geojson", fc.to_string()).unwrap();

         */

        let n_cells_contained = border.iter().fold(0, |mut acc, bc| {
            if filled_cells.contains(bc) {
                acc += 1;
            }
            acc
        });
        assert_eq!(border.len(), n_cells_contained);
    }
}

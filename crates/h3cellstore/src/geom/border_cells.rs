use crate::Error;
use cavalier_contours::polyline::{PlineCreation, PlineSource, PlineSourceMut, Polyline};
use geo::{Densify, EuclideanLength, Winding};
use geo_types::{Line, LineString, Polygon, Rect};
use h3ron::collections::HashSet;
use h3ron::{H3Cell, ToCoordinate, ToH3Cells, ToPolygon};
use ordered_float::OrderedFloat;

/// find the cells located directly within the exterior ring of the given polygon
///
/// The border cells are not guaranteed to be exactly one cell wide. Due to grid orientation
/// the line may be two cells wide at some places.
pub fn border_cells(poly: &Polygon, h3_resolution: u8) -> Result<HashSet<H3Cell>, Error> {
    let ext_ring = {
        let mut ext_ring = poly.exterior().clone();
        ext_ring.make_ccw_winding(); // make coord order deterministic so the offset direction is correct
        ext_ring
    };

    let cell_radius = max_cell_radius(&ext_ring, h3_resolution)?;
    let buffer_offset = cell_radius * 1.5;

    // small rects -> smaller grid_disks for H3 to generate
    let densification = cell_radius * 10.0;

    let mut out_cells = HashSet::default();
    for ext_line_segment in ext_ring.0.windows(2) {
        let line = Line::new(ext_line_segment[0], ext_line_segment[1]);

        let line_with_offset = {
            let mut pline = Polyline::with_capacity(2, false);
            pline.add(line.start.x, line.start.y, 0.0);
            pline.add(line.end.x, line.end.y, 0.0);

            let offsetted = pline.parallel_offset(buffer_offset);
            if offsetted.is_empty() {
                continue;
            }

            let offsetted_pline = &offsetted[0];
            if offsetted_pline.vertex_data.len() < 2 {
                continue;
            }
            let l = offsetted_pline.vertex_data.len();
            Line::new(
                (
                    offsetted_pline.vertex_data[0].x,
                    offsetted_pline.vertex_data[0].y,
                ),
                (
                    offsetted_pline.vertex_data[l - 1].x,
                    offsetted_pline.vertex_data[l - 1].y,
                ),
            )
        };

        let line_with_offset = line_with_offset.densify(densification);
        let line = line.densify(densification);

        for (line_window, line_with_offset_window) in
            line.0.windows(2).zip(line_with_offset.0.windows(2))
        {
            out_cells.extend(
                Rect::new(line_window[0], line_with_offset_window[1])
                    .to_h3_cells(h3_resolution)?
                    .iter(),
            );
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
    use geo_types::{Geometry, Rect};
    use h3ron::collections::HashSet;
    use h3ron::ToH3Cells;

    #[test]
    fn border_cells_within_rect() {
        let rect = Rect::new((30.0, 30.0), (50.0, 50.0));
        let h3_resolution = 7;

        let filled_cells = HashSet::from_iter(rect.to_h3_cells(h3_resolution).unwrap().iter());
        let border = border_cells(&rect.to_polygon(), h3_resolution).unwrap();
        dbg!(filled_cells.len(), border.len());
        assert!(border.len() > 100);

        // write geojson for visual inspection
        /**/
        {
            use geo_types::{GeometryCollection, Point};
            use h3ron::{ToCoordinate, ToPolygon};
            let mut geoms: Vec<Geometry> = vec![rect.into()];
            for bc in border.iter() {
                geoms.push(Point::from(bc.to_coordinate().unwrap()).into());
                geoms.push(bc.to_polygon().unwrap().into());
            }
            let gc = GeometryCollection::from(geoms);
            let fc = geojson::FeatureCollection::from(&gc);
            std::fs::write("/tmp/border.geojson", fc.to_string()).unwrap();
        }
        /**/

        let n_cells_contained = border.iter().fold(0, |mut acc, bc| {
            if filled_cells.contains(bc) {
                acc += 1;
            }
            acc
        });
        dbg!(n_cells_contained);
        assert!(border.len() <= n_cells_contained);
    }
}

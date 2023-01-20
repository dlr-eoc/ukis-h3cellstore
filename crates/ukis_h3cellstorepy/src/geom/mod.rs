use crate::error::IntoPyResult;
use geo_types::Geometry;
use numpy::{IntoPyArray, PyArray1};
use py_geo_interface::Geometry as GiGeometry;
use pyo3::prelude::PyModule;
use pyo3::prelude::*;
use pyo3::{wrap_pyfunction, PyResult, Python};
use ukis_h3cellstore::export::h3ron::collections::HashSet;
use ukis_h3cellstore::export::h3ron::Index;

/// find the cells located directly within the exterior ring of the given polygon
///
/// The border cells are not guaranteed to be exactly one cell wide. Due to grid orientation
/// the line may be two cells wide at some places.
///
/// `width`: Width of the border in (approx.) number of cells. Default: 1
#[pyfunction]
#[pyo3(signature = (geometry, h3_resolution, width=1))]
fn border_cells(
    py: Python,
    geometry: GiGeometry,
    h3_resolution: u8,
    width: u32,
) -> PyResult<Py<PyArray1<u64>>> {
    let width = Some(width);
    let cells = match &geometry.0 {
        Geometry::Polygon(poly) => {
            ukis_h3cellstore::geom::border_cells(poly, h3_resolution, width).into_pyresult()?
        }
        Geometry::MultiPolygon(mp) => {
            let mut hs = HashSet::default();
            for poly in mp {
                hs.extend(
                    ukis_h3cellstore::geom::border_cells(poly, h3_resolution, width)
                        .into_pyresult()?
                        .iter(),
                );
            }
            hs
        }
        _ => HashSet::default(),
    };

    // to numpy
    Ok(Vec::from_iter(cells.into_iter().map(|c| c.h3index()))
        .into_pyarray(py)
        .to_owned())
}

pub fn init_geom_submodule(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(border_cells, m)?)?;
    Ok(())
}

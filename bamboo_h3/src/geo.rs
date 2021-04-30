///
/// geospatial primitives and algorithms
///
use std::collections::HashMap;
use std::convert::TryInto;
use std::io::Cursor;
use std::iter::once;
use std::str::FromStr;

use geojson::GeoJson;
use h3ron::{H3Cell, Index, ToCoordinate};
use numpy::{Ix1, PyArray, PyReadonlyArray1};
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyBytes, PyTuple},
    PyResult,
};
use wkb::WKBReadExt;

use bamboo_h3_core::geo::algorithm::bounding_rect::BoundingRect;
use bamboo_h3_core::geo::algorithm::contains::Contains;
use bamboo_h3_core::geo::algorithm::intersects::Intersects;
use bamboo_h3_core::{geo_types as gt, ColVec, COL_NAME_H3INDEX};

use crate::columnset::{vec_to_numpy_owned, ColumnSet};
use crate::error::IntoPyResult;

/// a polygon
#[pyclass]
pub struct Polygon {
    pub(crate) inner: gt::Polygon<f64>,
}

#[pymethods]
impl Polygon {
    #[staticmethod]
    fn from_geojson(instr: &str) -> PyResult<Self> {
        let gj = GeoJson::from_str(instr)
            .map_err(|_| PyValueError::new_err("invalid geojson for polygon"))?;
        let gj_geom: geojson::Geometry = gj
            .try_into()
            .map_err(|_| PyValueError::new_err("geojson is not a geometry"))?;
        let poly: gt::Polygon<f64> = gj_geom
            .value
            .try_into()
            .map_err(|_| PyValueError::new_err("geojson is not a polygon"))?;
        Ok(Self { inner: poly })
    }

    #[staticmethod]
    fn from_wkb(wkb_data: &[u8]) -> PyResult<Self> {
        geotypes_polygon_from_wkb(wkb_data).map(|poly| Ok(Self { inner: poly }))?
    }

    /// convert the object to a geojson string
    fn to_geojson_str(&self) -> String {
        geojson::Value::from(&self.inner).to_string()
    }

    /// convert to WKB bytes
    fn to_wkb(&self, py: Python) -> PyResult<PyObject> {
        geotypes_polygon_to_pyobject(&self.inner, py)
    }

    /// check if the polygon contains the given point
    fn contains_point(&self, x: f64, y: f64) -> bool {
        self.inner.contains(&gt::Coordinate { x, y })
    }

    // taken from https://github.com/nmandery/h3ron/blob/master/h3ronpy/src/polygon.rs
    /// python __geo_interface__ spec: https://gist.github.com/sgillies/2217756
    #[getter]
    fn __geo_interface__(&self, py: Python) -> PyObject {
        let mut main = HashMap::new();
        main.insert("type".to_string(), "Polygon".to_string().into_py(py));
        main.insert("coordinates".to_string(), {
            let rings: Vec<_> = once(self.inner.exterior())
                .chain(self.inner.interiors().iter())
                .map(|ring| {
                    let r: Vec<_> = ring
                        .0
                        .iter()
                        .map(|c| PyTuple::new(py, &[c.x, c.y]).to_object(py))
                        .collect();
                    PyTuple::new(py, &r).to_object(py)
                })
                .collect();
            PyTuple::new(py, &rings).to_object(py)
        });

        main.to_object(py)
    }
}

impl From<gt::Polygon<f64>> for Polygon {
    fn from(gt_poly: gt::Polygon<f64>) -> Self {
        Self { inner: gt_poly }
    }
}

fn geotypes_polygon_from_wkb(wkb_data: &[u8]) -> PyResult<gt::Polygon<f64>> {
    let mut cursor = Cursor::new(wkb_data);
    match cursor.read_wkb() {
        Ok(geom) => match geom {
            gt::Geometry::Polygon(poly) => Ok(poly),
            _ => Err(PyValueError::new_err("unsupported geometry type")),
        },
        Err(e) => Err(PyValueError::new_err(format!(
            "could not deserialize from wkb: {:?}",
            e
        ))),
    }
}

fn geotypes_polygon_to_pyobject(poly: &gt::Polygon<f64>, py: Python) -> PyResult<PyObject> {
    let geom = gt::Geometry::Polygon(poly.clone());
    match wkb::geom_to_wkb(&geom) {
        Ok(d) => Ok(PyBytes::new(py, &d).to_object(py)),
        Err(e) => Err(PyValueError::new_err(format!(
            "could not serialize to wkb: {:?}",
            e
        ))),
    }
}

///
/// brute-force (no index) check a list of h3 indexes if
/// they are contained in polygons
#[pyclass]
pub struct H3IndexesContainedIn {
    h3indexes: Vec<u64>,
    h3indexes_coords: Vec<gt::Coordinate<f64>>,

    /// the box all points are contained in
    bounding_poly: Option<gt::Polygon<f64>>,
}

#[pymethods]
impl H3IndexesContainedIn {
    #[staticmethod]
    pub fn from_array(h3indexes: PyReadonlyArray1<u64>) -> PyResult<Self> {
        let h3indexes = h3indexes.as_array().to_vec();

        let mut h3indexes_coords = Vec::with_capacity(h3indexes.len());
        for h3index in h3indexes.iter() {
            h3indexes_coords.push(H3Cell::new(*h3index).to_coordinate())
        }
        let bounding_poly = gt::MultiPoint(
            h3indexes_coords
                .iter()
                .map(|coord| gt::Point(*coord))
                .collect(),
        )
        .bounding_rect()
        .map(|r| r.to_polygon());

        Ok(Self {
            h3indexes,
            h3indexes_coords,
            bounding_poly,
        })
    }

    /// perform a containment check and return a numpy array of the contained
    /// h3indexes.
    pub fn contained_h3indexes(&self, poly: &Polygon) -> PyResult<Py<PyArray<u64, Ix1>>> {
        // shortcut - is the whole bounding_poly inside the other poly, then there is no need
        // to check each of the points. Should be helpful when dealing with large
        // satellite footprints.
        if let Some(bounding_poly) = &self.bounding_poly {
            if poly.inner.contains(bounding_poly) {
                return Ok(vec_to_numpy_owned(self.h3indexes.clone()));
            }
        }
        let contained: Vec<_> = self
            .h3indexes
            .iter()
            .zip(self.h3indexes_coords.iter())
            .filter(|(_, c)| poly.inner.contains(*c))
            .map(|(h3index, _)| *h3index)
            .collect();

        Ok(vec_to_numpy_owned(contained))
    }
}

pub fn intersect_columnset_with_indexes(
    py: Python,
    cs: &ColumnSet,
    wkbs: Vec<&[u8]>,
    h3indexes: PyReadonlyArray1<u64>,
) -> PyResult<ColumnSet> {
    let geoms: Vec<_> = wkbs
        .iter()
        .map(|wkb| {
            let mut cursor = Cursor::new(wkb);
            wkb::wkb_to_geom(&mut cursor).into_pyresult()
        })
        .collect::<PyResult<Vec<bamboo_h3_core::geo_types::Geometry<f64>>>>()?;

    let h3index_coords: Vec<_> = h3indexes
        .as_array()
        .iter()
        .map(|h3index| {
            let index = H3Cell::new(*h3index);
            index.validate().into_pyresult()?;
            Ok((index.h3index(), index.to_coordinate()))
        })
        .collect::<PyResult<Vec<(u64, bamboo_h3_core::geo_types::Coordinate<f64>)>>>()?;

    let mut repetitions: Vec<usize> = Vec::with_capacity(wkbs.len());
    let mut out_h3indexes = Vec::with_capacity(wkbs.len());
    for geom in geoms.iter() {
        let mut reps = 0_usize;
        for (h3index, coord) in h3index_coords.iter() {
            if geom.intersects(coord) {
                reps += 1;
                out_h3indexes.push(*h3index)
            }
        }
        repetitions.push(reps);

        py.check_signals()?; // check for interrupts
    }
    let total_num: usize = repetitions.iter().sum();

    let mut out_columns = HashMap::new();
    for (col_name, colvec) in cs.inner.columns.iter() {
        let repeated = colvec
            .clone()
            .into_repeated_values(&repetitions, Some(total_num));
        out_columns.insert(col_name.clone(), repeated);
    }
    out_columns.insert(COL_NAME_H3INDEX.to_string(), ColVec::U64(out_h3indexes));
    Ok(out_columns.into())
}

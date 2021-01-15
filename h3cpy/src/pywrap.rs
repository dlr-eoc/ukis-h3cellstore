use std::convert::TryInto;
use std::str::FromStr;

use geo_types as gt;
use geo::algorithm::contains::Contains;
use geojson::GeoJson;
use h3ron::Index;
use pyo3::{
    prelude::*,
    exceptions::PyValueError,
    PyResult,
    types::{
        PyBytes
    },
};
use std::io::Cursor;
use wkb::WKBReadExt;

pub fn check_index_valid(index: &Index) -> PyResult<()> {
    if !index.is_valid() {
        Err(PyValueError::new_err(format!("invalid h3index given: {}", index.h3index())))
    } else {
        Ok(())
    }
}

pub fn intresult_to_pyresult<T>(res: std::result::Result<T, h3cpy_int::error::Error>) -> PyResult<T> {
    match res {
        Ok(v) => Ok(v),
        Err(e) => Err(PyValueError::new_err(e.to_string()))
    }
}


/// a polygon
#[pyclass]
pub struct Polygon {
    pub inner: gt::Polygon<f64>
}

#[pymethods]
impl Polygon {

    /// TODO: would be nice to support pythons __geo_interface__ or at least geojson-like dicts,
    ///       but only with little effort.

    #[staticmethod]
    fn from_geojson(instr: &str) -> PyResult<Self> {
        let gj = GeoJson::from_str(instr)
            .map_err(|_| PyValueError::new_err("invalid geojson for polygon"))?;
        let gj_geom: geojson::Geometry = gj.try_into()
            .map_err(|_| PyValueError::new_err("geojson is not a geometry"))?;
        let poly: gt::Polygon<f64> = gj_geom.value.try_into()
            .map_err(|_| PyValueError::new_err("geojson is not a polygon"))?;
        Ok(Self {
            inner: poly
        })
    }

    #[staticmethod]
    fn from_wkb(wkb_data: &[u8]) -> PyResult<Self> {
        let mut cursor = Cursor::new(wkb_data);
        match cursor.read_wkb() {
            Ok(geom) => match geom {
                gt::Geometry::Polygon(poly) => Ok(Self { inner: poly}),
                _ => Err(PyValueError::new_err("unsupported geometry type")),
            }
            Err(e) => Err(PyValueError::new_err(format!("could not deserialize from wkb: {:?}", e))),
        }
    }

    /// convert the object to a geojson string
    fn to_geojson_str(&self) -> PyResult<String> {
        Ok(geojson::Value::from(&self.inner).to_string())
    }

    /// convert to WKB bytes
    fn to_wkb<'py>(&self, py: Python<'py>) -> PyResult<&'py PyBytes> {
        let geom = gt::Geometry::Polygon(self.inner.clone());
        match wkb::geom_to_wkb(&geom) {
            Ok(d) => Ok(PyBytes::new(py, &d)),
            Err(e) => Err(PyValueError::new_err(format!("could not serialize to wkb: {:?}", e))),
        }
    }

    /// check if the polygon contains the given point
    fn contains_point(&self, x: f64, y: f64) -> bool {
        self.inner.contains(&gt::Coordinate { x, y })
    }
}
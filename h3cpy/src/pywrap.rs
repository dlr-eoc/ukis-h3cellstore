use std::convert::TryInto;
use std::str::FromStr;

use geo_types::Polygon;
use geojson::GeoJson;
use h3ron::Index;
use pyo3::{
    exceptions::{PyTypeError, PyValueError},
    PyAny,
    PyResult,
    types::PyString,
};

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


/// convert a python object to a polygon
///
/// TODO: would be nice to support pythons __geo_interface__ or at least geojson-like dicts,
///       but only with little effort.
pub fn polygon_from_python(input: &PyAny) -> PyResult<Polygon<f64>> {
    if let Ok(py_str) = input.downcast::<PyString>() {
        // its a string, so lets assume it is a geojson string
        let gj = GeoJson::from_str(py_str.to_str()?)
            .map_err(|_| PyValueError::new_err("invalid geojson for polygon"))?;
        let gj_geom: geojson::Geometry = gj.try_into()
            .map_err(|_| PyValueError::new_err("geojson is not a geometry"))?;
        let poly: Polygon<f64> = gj_geom.value.try_into()
            .map_err(|_| PyValueError::new_err("geojson is not a polygon"))?;
        Ok(poly)
    } else {
        Err(PyTypeError::new_err("unsupported input for polygon"))
    }
}

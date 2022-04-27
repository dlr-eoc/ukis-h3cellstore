use crate::error::{IntoPyResult, ToCustomPyErr};
use h3cellstore::export::arrow_h3::export::h3ron::Index;
use numpy::PyReadonlyArray1;
use pyo3::exceptions::PyValueError;
use pyo3::types::PyDict;
use pyo3::{FromPyObject, PyResult};

pub(crate) fn indexes_from_numpy<IX>(arr: PyReadonlyArray1<u64>) -> PyResult<Vec<IX>>
where
    IX: TryFrom<u64> + Index,
    <IX as TryFrom<u64>>::Error: ToCustomPyErr,
{
    arr.as_array()
        .iter()
        .map(|h3index| IX::try_from(*h3index))
        .collect::<Result<Vec<_>, _>>()
        .into_pyresult()
}

pub(crate) fn extract_dict_item<'a, D, K>(dict: &'a PyDict, key: K) -> PyResult<Option<D>>
where
    D: FromPyObject<'a>,
    K: AsRef<str>,
{
    if let Some(okp) = dict.get_item(key.as_ref()) {
        Ok(Some(okp.extract::<D>().map_err(|_e| {
            PyValueError::new_err(format!("Unsupported value for key {}", key.as_ref()))
        })?))
    } else {
        Ok(None)
    }
}

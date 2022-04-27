use crate::error::{IntoPyResult, ToCustomPyErr};
use h3cellstore::export::arrow_h3::export::h3ron::Index;
use numpy::PyReadonlyArray1;
use pyo3::PyResult;

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

use crate::error::IntoPyResult;
use h3cellstore::export::arrow_h3::export::h3ron::H3Cell;
use numpy::PyReadonlyArray1;
use pyo3::PyResult;

pub(crate) fn cells_from_numpy(arr: PyReadonlyArray1<u64>) -> PyResult<Vec<H3Cell>> {
    arr.as_array()
        .iter()
        .map(|h3index| H3Cell::try_from(*h3index))
        .collect::<Result<Vec<_>, _>>()
        .into_pyresult()
}

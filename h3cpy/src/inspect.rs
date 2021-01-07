use pyo3::class::basic::CompareOp;
use pyo3::class::basic::PyObjectProtocol;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use h3cpy_int::compacted_tables as ct;

#[pyclass]
#[derive(Clone)]
pub struct CompactedTable {
    pub table: ct::Table
}

#[pymethods]
impl CompactedTable {
    #[getter]
    pub fn is_compacted(&self) -> PyResult<bool> {
        Ok(self.table.spec.is_compacted)
    }

    #[getter]
    pub fn basename(&self) -> PyResult<String> {
        Ok(self.table.basename.clone())
    }

    #[getter]
    pub fn h3_resolution(&self) -> PyResult<u8> {
        Ok(self.table.spec.h3_resolution)
    }

    #[staticmethod]
    pub fn parse(instr: &str) -> PyResult<CompactedTable> {
        if let Some(table) = ct::Table::parse(instr) {
            Ok(CompactedTable { table })
        } else {
            Err(PyValueError::new_err("could not parse table name"))
        }
    }
}


#[pyproto]
impl<'p> PyObjectProtocol<'p> for CompactedTable {
    fn __richcmp__(&self, other: CompactedTable, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self.table == other.table),
            CompareOp::Ne => Ok(self.table != other.table),
            _ => Err(PyNotImplementedError::new_err("not impl")),
        }
    }
}


#[pyclass]
#[derive(Clone)]
pub struct TableSet {
    pub(crate) inner: ct::TableSet
}

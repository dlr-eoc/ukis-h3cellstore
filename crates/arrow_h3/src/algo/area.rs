use h3ron::H3Cell;
use polars_core::prelude::{NamedFrom, Series};

use crate::algo::IterSeriesIndexes;
use crate::{Error, H3DataFrame};

// TODO: assumes cells
pub trait H3CellArea {
    fn h3cell_areas_m2(&self) -> Result<Vec<f64>, Error>;
    fn h3cell_areas_rads2(&self) -> Result<Vec<f64>, Error>;
}

impl H3CellArea for Series {
    fn h3cell_areas_m2(&self) -> Result<Vec<f64>, Error> {
        self.iter_indexes::<H3Cell>()?
            .map(|cell_result| match cell_result {
                Ok(cell) => cell.area_m2().map_err(|e| e.into()),
                Err(e) => Err(e),
            })
            .collect::<Result<Vec<_>, _>>()
    }

    fn h3cell_areas_rads2(&self) -> Result<Vec<f64>, Error> {
        self.iter_indexes::<H3Cell>()?
            .map(|cell_result| match cell_result {
                Ok(cell) => cell.area_rads2().map_err(|e| e.into()),
                Err(e) => Err(e),
            })
            .collect::<Result<Vec<_>, _>>()
    }
}

impl H3CellArea for H3DataFrame {
    fn h3cell_areas_m2(&self) -> Result<Vec<f64>, Error> {
        self.index_series()?.h3cell_areas_m2()
    }

    fn h3cell_areas_rads2(&self) -> Result<Vec<f64>, Error> {
        self.index_series()?.h3cell_areas_rads2()
    }
}

pub trait AppendH3CellAreaColumn {
    fn append_h3cell_area_m2_column<S>(&mut self, column_name: S) -> Result<(), Error>
    where
        Self: Sized,
        S: AsRef<str>;

    fn append_h3cell_area_rads2_column<S>(&mut self, column_name: S) -> Result<(), Error>
    where
        Self: Sized,
        S: AsRef<str>;
}

impl AppendH3CellAreaColumn for H3DataFrame {
    fn append_h3cell_area_m2_column<S>(&mut self, column_name: S) -> Result<(), Error>
    where
        Self: Sized,
        S: AsRef<str>,
    {
        let areas = self.h3cell_areas_m2()?;
        self.dataframe
            .with_column(Series::new(column_name.as_ref(), areas))?;

        Ok(())
    }

    fn append_h3cell_area_rads2_column<S>(&mut self, column_name: S) -> Result<(), Error>
    where
        Self: Sized,
        S: AsRef<str>,
    {
        let areas = self.h3cell_areas_rads2()?;
        self.dataframe
            .with_column(Series::new(column_name.as_ref(), areas))?;

        Ok(())
    }
}

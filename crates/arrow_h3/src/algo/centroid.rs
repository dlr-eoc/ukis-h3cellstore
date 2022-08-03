use crate::algo::IterSeriesIndexes;
use crate::{Error, H3DataFrame};
use geo_types::Coordinate;
use h3ron::{H3Cell, ToCoordinate};
use polars_core::prelude::NamedFrom;
use polars_core::series::Series;

// TODO: assumes cells
pub trait H3CellCentroids {
    fn h3cell_centroids(&self) -> Result<Vec<Coordinate<f64>>, Error>;
}

impl H3CellCentroids for Series {
    fn h3cell_centroids(&self) -> Result<Vec<Coordinate<f64>>, Error> {
        self.iter_indexes::<H3Cell>()?
            .map(|cell_result| match cell_result {
                Ok(cell) => cell.to_coordinate().map_err(|e| e.into()),
                Err(e) => Err(e),
            })
            .collect::<Result<Vec<_>, _>>()
    }
}

impl H3CellCentroids for H3DataFrame {
    fn h3cell_centroids(&self) -> Result<Vec<Coordinate<f64>>, Error> {
        self.index_series()?.h3cell_centroids()
    }
}

pub trait AppendH3CellCentroidColumns {
    /// Also handles partially compacted and pre-compacted data
    fn append_h3cell_centroid_columns<SX, SY>(
        &mut self,
        column_name_x: SX,
        column_name_y: SY,
    ) -> Result<(), Error>
    where
        Self: Sized,
        SX: AsRef<str>,
        SY: AsRef<str>;
}

impl AppendH3CellCentroidColumns for H3DataFrame {
    fn append_h3cell_centroid_columns<SX, SY>(
        &mut self,
        column_name_x: SX,
        column_name_y: SY,
    ) -> Result<(), Error>
    where
        Self: Sized,
        SX: AsRef<str>,
        SY: AsRef<str>,
    {
        let capacity = self.dataframe.shape().0;
        let (vec_x, vec_y) = self.h3cell_centroids()?.iter().fold(
            (Vec::with_capacity(capacity), Vec::with_capacity(capacity)),
            |mut acc, coord| {
                acc.0.push(coord.x);
                acc.1.push(coord.y);
                acc
            },
        );

        self.dataframe
            .with_column(Series::new(column_name_x.as_ref(), vec_x))?
            .with_column(Series::new(column_name_y.as_ref(), vec_y))?;
        Ok(())
    }
}

use h3ron::{H3Cell, Index};
use polars_core::prelude::NamedFrom;
use polars_core::series::Series;

use crate::algo::IterSeriesIndexes;
use crate::{Error, H3DataFrame};

// TODO: assumes cells
pub trait ObtainH3Resolutions {
    fn h3_resolutions(&self) -> Result<Vec<u8>, Error>;

    fn h3_resolutions_series(&self) -> Result<Series, Error> {
        Ok(Series::new("resolutions", self.h3_resolutions()?))
    }
}

impl ObtainH3Resolutions for Series {
    fn h3_resolutions(&self) -> Result<Vec<u8>, Error> {
        self.iter_indexes::<H3Cell>()?
            .map(|cell_result| cell_result.map(|cell| cell.resolution()))
            .collect::<Result<Vec<_>, _>>()
    }
}

impl ObtainH3Resolutions for H3DataFrame {
    fn h3_resolutions(&self) -> Result<Vec<u8>, Error> {
        self.index_series()?.h3_resolutions()
    }
}

pub trait AppendResolutionColumn {
    /// Also handles partially compacted and pre-compacted data
    fn append_resolution_column<S>(&mut self, column_name: S) -> Result<(), Error>
    where
        Self: Sized,
        S: AsRef<str>;
}

impl AppendResolutionColumn for H3DataFrame {
    fn append_resolution_column<S>(&mut self, column_name: S) -> Result<(), Error>
    where
        Self: Sized,
        S: AsRef<str>,
    {
        let resolutions = self.h3_resolutions()?;
        self.dataframe
            .with_column(Series::new(column_name.as_ref(), resolutions))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::algo::tests::make_h3_dataframe;
    use crate::algo::AppendResolutionColumn;

    #[test]
    fn append_resolution() {
        let mut h3df = make_h3_dataframe(5, None).unwrap();
        h3df.append_resolution_column("res").unwrap();
        let res_column = h3df.dataframe.column("res").unwrap().unique().unwrap();
        let mut iter = res_column.u8().unwrap().into_iter();
        assert_eq!(iter.size_hint().0, 1);
        assert_eq!(iter.next(), Some(Some(5u8)));
    }
}

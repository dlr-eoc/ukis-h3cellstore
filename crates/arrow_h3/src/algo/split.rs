use polars_core::series::ChunkCompare;
use tracing::{span, Level};

use crate::algo::ObtainH3Resolutions;
use crate::{Error, H3DataFrame};

pub trait SplitByH3Resolution {
    fn split_by_h3_resolution(self) -> Result<Vec<(u8, Self)>, Error>
    where
        Self: Sized;
}

const RESSPLIT_HELPER_COL_NAME: &str = "_ressplit_helper";

impl SplitByH3Resolution for H3DataFrame {
    fn split_by_h3_resolution(mut self) -> Result<Vec<(u8, Self)>, Error>
    where
        Self: Sized,
    {
        let span = span!(
            Level::DEBUG,
            "Splitting H3DataFrame by H3 resolutions",
            n_rows = self.dataframe.shape().0,
            n_columns = self.dataframe.shape().1
        );
        let _enter = span.enter();

        let mut contained_resolutions = self.h3_resolutions_series()?;
        contained_resolutions.rename(RESSPLIT_HELPER_COL_NAME);

        let distinct_resolutions: Vec<u8> = contained_resolutions
            .unique()?
            .u8()?
            .into_iter()
            .flatten()
            .collect();

        match distinct_resolutions.len() {
            0 => Ok(vec![]),
            1 => Ok(vec![(distinct_resolutions[0], self)]),
            _ => {
                // TODO: this could probably be more efficient

                self.dataframe.with_column(contained_resolutions)?;
                let mut out_h3dfs = Vec::with_capacity(distinct_resolutions.len());
                for h3_resolution in distinct_resolutions {
                    let filtered = self
                        .dataframe
                        .filter(
                            &self
                                .dataframe
                                .column(RESSPLIT_HELPER_COL_NAME)?
                                .equal(h3_resolution),
                        )?
                        .drop(RESSPLIT_HELPER_COL_NAME)?;

                    out_h3dfs.push((
                        h3_resolution,
                        (filtered, self.h3index_column_name.clone()).try_into()?,
                    ))
                }

                Ok(out_h3dfs)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use h3ron::{H3Cell, Index};
    use polars_core::frame::DataFrame;
    use polars_core::prelude::{NamedFrom, Series};

    use crate::algo::SplitByH3Resolution;
    use crate::H3DataFrame;

    #[test]
    fn split_three_frames() {
        let series = Series::new(
            "idx",
            vec![
                H3Cell::from_coordinate((12.7, 10.1).into(), 7)
                    .unwrap()
                    .h3index() as u64,
                H3Cell::from_coordinate((12.7, 4.1).into(), 8)
                    .unwrap()
                    .h3index() as u64,
                H3Cell::from_coordinate((12.7, 7.1).into(), 8)
                    .unwrap()
                    .h3index() as u64,
                H3Cell::from_coordinate((2.7, 10.1).into(), 5)
                    .unwrap()
                    .h3index() as u64,
            ],
        );
        let h3df =
            H3DataFrame::from_dataframe(DataFrame::new(vec![series]).unwrap(), "idx").unwrap();

        let splitted = h3df.split_by_h3_resolution().unwrap();
        assert_eq!(splitted.len(), 3);
        for (h3_resolution, h3df) in splitted {
            let num_rows_expected = if h3_resolution == 8 { 2 } else { 1 };
            assert_eq!(h3df.dataframe.shape(), (num_rows_expected, 1));
        }
    }
}

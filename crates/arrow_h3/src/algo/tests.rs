use h3ron::H3Cell;
use polars_core::prelude::{NamedFrom, Series};

use crate::{Error, H3DataFrame};

pub(crate) fn make_h3_dataframe(
    h3_resolution: u8,
    value: Option<u32>,
) -> Result<H3DataFrame, Error> {
    let mut h3df = H3DataFrame::from_cell_iter(
        H3Cell::from_coordinate((10.0, 20.0).into(), h3_resolution)?
            .grid_disk(10)?
            .iter()
            .chain(
                H3Cell::from_coordinate((45.0, 45.0).into(), h3_resolution)?
                    .grid_disk(3)?
                    .iter(),
            ),
        "cell_h3index",
    )?;
    if let Some(value) = value {
        let count = h3df.dataframe.shape().0;
        h3df.dataframe.with_column(Series::new(
            "value",
            (0..count).map(|_| value).collect::<Vec<_>>(),
        ))?;
    }
    Ok(h3df)
}

use h3ron::H3Cell;
use polars_core::prelude::{DataFrame, NamedFrom, Series};

use crate::series::to_index_series;
use crate::{Error, H3DataFrame};

pub(crate) fn make_h3_dataframe(
    h3_resolution: u8,
    value: Option<u32>,
) -> Result<H3DataFrame, Error> {
    let cell_h3indexes = to_index_series(
        "cell_h3index",
        H3Cell::from_coordinate((10.0, 20.0).into(), h3_resolution)?
            .grid_disk(10)?
            .iter()
            .chain(
                H3Cell::from_coordinate((45.0, 45.0).into(), h3_resolution)?
                    .grid_disk(3)?
                    .iter(),
            ),
    );
    let count = cell_h3indexes.len();
    let mut series_vec = vec![cell_h3indexes];

    if let Some(value) = value {
        series_vec.push(Series::new(
            "value",
            (0..count).map(|_| value).collect::<Vec<_>>(),
        ));
    }
    let df = DataFrame::new(series_vec)?;
    Ok(H3DataFrame::from_dataframe(df, "cell_h3index")?)
}

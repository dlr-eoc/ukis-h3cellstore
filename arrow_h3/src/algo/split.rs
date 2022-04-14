use crate::{Error, H3DataFrame};

pub trait SplitByH3Resolution {
    fn split_by_h3_resolution(&self) -> Result<Vec<Self>, Error>
    where
        Self: Sized;
}

impl SplitByH3Resolution for H3DataFrame {
    fn split_by_h3_resolution(&self) -> Result<Vec<Self>, Error>
    where
        Self: Sized,
    {
        todo!()
    }
}

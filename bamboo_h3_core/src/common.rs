use crate::error::Error;
use h3ron::H3_MAX_RESOLUTION;

pub trait Named {
    fn name(&self) -> &'static str;
}

pub fn ordered_h3_resolutions(h3res_slice: &[u8]) -> Result<Vec<u8>, Error> {
    let mut cleaned = vec![];
    for res in h3res_slice.iter() {
        check_h3_resolution(*res)?;
        cleaned.push(*res);
    }
    cleaned.sort_unstable();
    cleaned.dedup();
    Ok(cleaned)
}

pub fn check_h3_resolution(r: u8) -> Result<(), Error> {
    if r > H3_MAX_RESOLUTION {
        Err(Error::InvalidH3Resolution(r))
    } else {
        Ok(())
    }
}

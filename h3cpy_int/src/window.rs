use std::iter::FromIterator;

use crate::compacted_tables::TableSet;

/// find the resolution generate coarser h3-indexes to access the tableset without needing to fetch more
/// than max_indexes_per_batch indexes per batch.
///
/// That resolution must be a base resolution
fn batch_index_resolution(table_set: &TableSet, target_h3_resolution: u8, max_indexes_per_batch: u32) -> u8 {
    let mut resolutions = Vec::from_iter(table_set.base_h3_resolutions
        .iter()
        .filter(|r| **r < target_h3_resolution)
        .map(|r| r.clone()));
    resolutions.sort();

    let mut batch_h3_resolution = target_h3_resolution;
    for r in resolutions {
        if 7_u32.pow((target_h3_resolution - r) as u32) <= max_indexes_per_batch {
            batch_h3_resolution = r;
            break;
        }
    }
    batch_h3_resolution
}


#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::iter::FromIterator;

    use crate::compacted_tables::TableSet;
    use crate::window::batch_index_resolution;

    #[test]
    fn test_batch_index_resolution() {
        let ts = TableSet {
            basename: "t1".to_string(),
            base_h3_resolutions: {
                let mut hs = HashSet::new();
                for r in 0..=6 {
                    hs.insert(r);
                }
                hs
            },
            compacted_h3_resolutions: Default::default(),
        };

        assert_eq!(
            batch_index_resolution(&ts, 6, 1000),
            3
        );
    }
}

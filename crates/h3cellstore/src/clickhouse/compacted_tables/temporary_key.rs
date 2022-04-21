use chrono::Local;
use rand::{thread_rng, Rng};

#[derive(Eq, PartialEq, Clone, Debug)]
pub struct TemporaryKey {
    unix_timestamp_secs: u64,
    unix_timestamp_millis: u16,
    random_part: u16,
}

impl TemporaryKey {
    /// generate a somewhat unique key for temporary tables
    ///
    /// The time when the key has been generated should be visible from the key itself.
    pub fn new() -> Self {
        let mut rng = thread_rng();
        let ts_millis = Local::now().naive_utc().timestamp_millis().abs() as u64;
        Self {
            unix_timestamp_secs: ts_millis / 1000,
            unix_timestamp_millis: (ts_millis % 1000) as u16,
            random_part: rng.gen(),
        }
    }
}

impl ToString for TemporaryKey {
    fn to_string(&self) -> String {
        format!(
            "{}_{}_{}",
            self.unix_timestamp_secs, self.unix_timestamp_millis, self.random_part
        )
    }
}

impl Default for TemporaryKey {
    fn default() -> Self {
        TemporaryKey::new()
    }
}

#[cfg(test)]
mod tests {
    use super::TemporaryKey;

    #[test]
    fn temporary_key_is_unique() {
        dbg!(TemporaryKey::new());
        assert_ne!(
            TemporaryKey::new().to_string(),
            TemporaryKey::new().to_string()
        );
    }
}

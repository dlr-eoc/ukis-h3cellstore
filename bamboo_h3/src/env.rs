use std::cmp::min;
use std::fmt::Display;
use std::str::FromStr;

/// Number of ClickHouse threads to use during window iteration.
/// The more threads are used, the higher the load and memory requirements in the db
/// server will be.
/// As this is used for mostly non-timecritical preloading, the number can be quite low.
const ENV_NAME_WINDOW_NUM_CLICKHOUSE_THREADS: &str = "BAMBOO_WINDOW_NUM_CLICKHOUSE_THREADS";

/// default number of clickhouse window threads
const DEFAULT_WINDOW_NUM_CLICKHOUSE_THREADS: u8 = 2;

/// Number of concurrent queries to use to preload data for the next windows from ClickHouse.
const ENV_NAME_WINDOW_NUM_CONCURRENT_PRELOAD_QUERIES: &str =
    "BAMBOO_WINDOW_NUM_CONCURRENT_PRELOAD_QUERIES";

/// Default number of concurrent queries to use for preloading.
const DEFAULT_WINDOW_NUM_CONCURRENT_PRELOAD_QUERIES: u8 = 3;

pub fn window_num_clickhouse_threads() -> u8 {
    get_numeric_env_with_default_and_min(
        ENV_NAME_WINDOW_NUM_CLICKHOUSE_THREADS,
        DEFAULT_WINDOW_NUM_CLICKHOUSE_THREADS,
        1,
    )
}

pub fn window_num_concurrent_queries() -> u8 {
    get_numeric_env_with_default_and_min(
        ENV_NAME_WINDOW_NUM_CONCURRENT_PRELOAD_QUERIES,
        DEFAULT_WINDOW_NUM_CONCURRENT_PRELOAD_QUERIES,
        1,
    )
}

fn get_numeric_env_with_default_and_min<T>(env_name: &str, default: T, min_value: T) -> T
where
    T: Display + FromStr + Ord,
{
    match std::env::var(env_name) {
        Ok(env_value) => match env_value.parse() {
            Ok(value) => {
                let value_with_min = min(value, min_value);
                log::debug!("Using {}={}", env_name, value_with_min);
                value_with_min
            }
            Err(_) => {
                log::debug!(
                    "Unable to parse {}. Using the default {}={}",
                    env_name,
                    env_name,
                    default,
                );
                default
            }
        },
        Err(_) => {
            log::debug!("Using the default {}={}", env_name, default);
            default
        }
    }
}

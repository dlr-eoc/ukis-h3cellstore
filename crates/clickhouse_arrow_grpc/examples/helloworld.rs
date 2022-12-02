use chrono::{NaiveDate, NaiveDateTime};
use polars::prelude::{DataFrame, NamedFrom};
use polars::series::Series;

use clickhouse_arrow_grpc::{ArrowInterface, Client, QueryInfo};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt::init();

    let mut client = Client::connect("http://127.0.0.1:9100").await?;

    // query
    let df = client
        .execute_into_dataframe(QueryInfo {
            query: r#"
            select 'string-äöü-utf8?' s1, 
                cast(name as Text),
                cast(1 as UInt64) as jkj,
                cast(now() as DateTime) as ts_datetime,
                toDateTime(now(), 'Asia/Istanbul') AS ts_datetime_tz,
                cast(now() as Date) as ts_date,
                cast(now() as DateTime64) as ts64,
                cast(1 as UInt8) as some_u8
            from tables"#
                .to_string(),
            database: "system".to_string(),
            ..Default::default()
        })
        .await?;
    dbg!(df);

    let play_db = "play";

    client
        .execute_query_checked(QueryInfo {
            query: format!("create database if not exists {}", play_db),
            ..Default::default()
        })
        .await?;

    client
        .execute_query_checked(QueryInfo {
            query: "drop table if exists test_insert".to_string(),
            database: play_db.to_string(),
            ..Default::default()
        })
        .await?;

    client
        .execute_query_checked(QueryInfo {
            query: r#"create table if not exists test_insert (
            v1 UInt64, 
            v2 Float32, 
            t1 text, 
            c_datetime64 DateTime64,
            c_datetime DateTime,
            c_date Date,
            b bool
            ) ENGINE Memory"#
                .to_string(),
            database: play_db.to_string(),
            ..Default::default()
        })
        .await?;

    let test_df = make_dataframe(40)?;
    client
        .insert_dataframe(play_db, "test_insert", test_df)
        .await?;
    let df2 = client
        .execute_into_dataframe(QueryInfo {
            query: "select *, version() as clickhouse_version from test_insert".to_string(),
            database: play_db.to_string(),
            ..Default::default()
        })
        .await?;
    dbg!(df2);

    Ok(())
}

fn make_dataframe(df_len: usize) -> anyhow::Result<DataFrame> {
    let test_df = DataFrame::new(vec![
        Series::new("v1", (0..df_len).map(|v| v as u64).collect::<Vec<_>>()),
        Series::new(
            "v2",
            (0..df_len).map(|v| v as f32 * 1.3).collect::<Vec<_>>(),
        ),
        Series::new(
            "t1",
            (0..df_len)
                .map(|v| format!("something-{}", v))
                .collect::<Vec<_>>(),
        ),
        Series::new(
            "c_datetime64",
            (0..df_len)
                .map(|v| {
                    NaiveDateTime::from_timestamp_opt((v as i64).pow(2), 0)
                        .expect("invalid timestamp")
                })
                .collect::<Vec<_>>(),
        ),
        Series::new(
            "c_date",
            (0..df_len)
                .map(|_| NaiveDate::from_ymd_opt(2000, 5, 23).expect("invalid date"))
                .collect::<Vec<_>>(),
        ),
        Series::new(
            "c_datetime",
            (0..df_len)
                .map(|_| {
                    NaiveDate::from_ymd_opt(2000, 5, 23)
                        .expect("invalid date")
                        .and_hms_opt(12, 13, 14)
                        .expect("invalid datetime")
                })
                .collect::<Vec<_>>(),
        ),
        Series::new("b", (0..df_len).map(|v| v % 2 == 0).collect::<Vec<_>>()),
    ])?;
    Ok(test_df)
}

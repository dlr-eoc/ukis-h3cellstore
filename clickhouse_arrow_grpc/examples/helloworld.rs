use chrono::NaiveDateTime;
use polars::prelude::{DataFrame, NamedFrom};
use polars::series::Series;

use clickhouse_arrow_grpc::{ArrowInterface, ClickHouseClient, QueryInfo};

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let mut client = ClickHouseClient::connect("http://127.0.0.1:9100")
        .await?
        .send_gzip()
        .accept_gzip();

    // query
    let df = client.execute_into_dataframe(
        QueryInfo {
            query: "select 'string-äöü-utf8?' s1, cast(name as Text), cast(1 as UInt64) as jkj, cast(now() as DateTime) as ts_datetime, cast(now() as Date) as ts_date, cast(now() as DateTime64) as ts64 from tables"
                .to_string(),
            database: "system".to_string(),
            ..Default::default()
        },
    )
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

    client.execute_query_checked(
        QueryInfo {
            query: "create table if not exists test_insert (v1 UInt64, v2 Float32 , t1 text, timestamp DateTime64) ENGINE Memory"
                .to_string(),
            database: play_db.to_string(),
            ..Default::default()
        },
    )
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

fn make_dataframe(df_len: usize) -> eyre::Result<DataFrame> {
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
            "timestamp",
            (0..df_len)
                .map(|v| NaiveDateTime::from_timestamp((v as i64).pow(2), 0))
                .collect::<Vec<_>>(),
        ),
    ])?;
    Ok(test_df)
}

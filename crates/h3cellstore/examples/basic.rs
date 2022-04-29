use chrono::Local;
use geo_types::Coordinate;

use arrow_h3::export::h3ron::H3Cell;
use arrow_h3::export::polars::frame::DataFrame;
use arrow_h3::export::polars::prelude::NamedFrom;
use arrow_h3::export::polars::series::Series;
use arrow_h3::series::to_index_series;
use arrow_h3::H3DataFrame;
use h3cellstore::clickhouse::compacted_tables::schema::{
    AggregationMethod, ClickhouseDataType, ColumnDefinition, CompactedTableSchema,
    CompactedTableSchemaBuilder, SimpleColumn, TemporalPartitioning,
};
use h3cellstore::clickhouse::compacted_tables::{CompactedTablesStore, InsertOptions};
use h3cellstore::clickhouse::compacted_tables::{TableSetQuery, COL_NAME_H3INDEX};
use h3cellstore::export::clickhouse_arrow_grpc::{ArrowInterface, ClickHouseClient, QueryInfo};

const MAX_H3_RES: u8 = 5;

fn okavango_delta_schema() -> eyre::Result<CompactedTableSchema> {
    let schema = CompactedTableSchemaBuilder::new("okavango_delta")
        .h3_base_resolutions((0..=MAX_H3_RES).collect())
        .temporal_partitioning(TemporalPartitioning::Month)
        .add_column(
            "elephant_count",
            ColumnDefinition::WithAggregation(
                SimpleColumn::new(ClickhouseDataType::UInt32, None),
                AggregationMethod::Sum,
            ),
        )
        .add_column(
            "observed_on",
            ColumnDefinition::Simple(SimpleColumn::new(ClickhouseDataType::DateTime64, Some(0))),
        )
        .build()?;
    Ok(schema)
}

fn make_h3dataframe(center: Coordinate<f64>) -> eyre::Result<H3DataFrame> {
    let index_series = to_index_series(
        COL_NAME_H3INDEX,
        H3Cell::from_coordinate(center, MAX_H3_RES)?
            .grid_disk(10)?
            .iter(),
    );

    let num_cells = index_series.len();
    let df = DataFrame::new(vec![
        index_series,
        Series::new(
            "elephant_count",
            (0..num_cells).map(|_| 2_u32).collect::<Vec<_>>(),
        ),
        Series::new(
            "observed_on",
            (0..num_cells)
                .map(|_| Local::now().naive_local())
                .collect::<Vec<_>>(),
        ),
    ])?;

    Ok(H3DataFrame::from_dataframe(df, COL_NAME_H3INDEX)?)
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt::init();

    let center = Coordinate::from((22.8996, -19.3325));

    let mut client = ClickHouseClient::connect("http://127.0.0.1:9100")
        .await?
        .send_gzip()
        .accept_gzip();

    let play_db = "play";
    client
        .execute_query_checked(QueryInfo {
            query: format!("create database if not exists {}", play_db),
            ..Default::default()
        })
        .await?;

    let schema = okavango_delta_schema()?;
    client.drop_tableset(&play_db, &schema.name).await?;
    //return Ok(());
    client.create_tableset(&play_db, &schema).await?;

    let tablesets = client.list_tablesets(&play_db).await?;
    assert!(tablesets.contains_key(&schema.name));

    let h3df = make_h3dataframe(center)?;

    client
        .insert_h3dataframe_into_tableset(
            &play_db,
            &schema,
            h3df,
            InsertOptions {
                max_num_rows_per_chunk: 20,
                ..Default::default()
            },
        )
        .await?;

    let queried_df = client
        .query_tableset_cells(
            &play_db,
            &schema.name,
            TableSetQuery::AutoGenerated,
            vec![H3Cell::from_coordinate(center, MAX_H3_RES - 1)?],
            MAX_H3_RES,
        )
        .await?;
    dbg!(&queried_df);
    assert_eq!(queried_df.dataframe.shape().0, 7);

    client.drop_tableset(&play_db, "okavango_delta").await?;
    assert!(!client
        .list_tablesets(&play_db)
        .await?
        .contains_key("okavango_delta"));

    Ok(())
}
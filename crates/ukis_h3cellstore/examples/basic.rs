use chrono::Local;
use geo_types::Coord;
use h3ron::H3Cell;
use h3ron_polars::frame::H3DataFrame;
use h3ron_polars::FromIndexIterator;
use polars::prelude::{DataFrame, NamedFrom, Series};

use ukis_h3cellstore::clickhouse::compacted_tables::schema::{
    AggregationMethod, ClickhouseDataType, ColumnDefinition, CompactedTableSchema,
    CompactedTableSchemaBuilder, SimpleColumn, TemporalPartitioning,
};
use ukis_h3cellstore::clickhouse::compacted_tables::COL_NAME_H3INDEX;
use ukis_h3cellstore::clickhouse::compacted_tables::{
    CompactedTablesStore, InsertOptions, QueryOptions,
};
use ukis_h3cellstore::export::ukis_clickhouse_arrow_grpc::{ArrowInterface, Client, QueryInfo};

const MAX_H3_RES: u8 = 5;

fn okavango_delta_schema() -> anyhow::Result<CompactedTableSchema> {
    let schema = CompactedTableSchemaBuilder::new("okavango_delta")
        .h3_base_resolutions((0..=MAX_H3_RES).collect())
        .temporal_partitioning(TemporalPartitioning::Months(1))
        .add_column(
            "elephant_count",
            ColumnDefinition::WithAggregation(
                SimpleColumn::new(ClickhouseDataType::UInt32, None, None, false),
                AggregationMethod::Sum,
            ),
        )
        .add_column(
            "observed_on",
            ColumnDefinition::Simple(SimpleColumn::new(
                ClickhouseDataType::DateTime64,
                Some(0),
                None,
                false,
            )),
        )
        .build()?;
    Ok(schema)
}

fn make_h3dataframe(center: Coord<f64>) -> anyhow::Result<H3DataFrame<H3Cell>> {
    let mut index_series = Series::from_index_iter(
        H3Cell::from_coordinate(center, MAX_H3_RES)?
            .grid_disk(10)?
            .iter(),
    );
    index_series.rename(COL_NAME_H3INDEX);
    dbg!(&index_series);

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
async fn main() -> anyhow::Result<()> {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt::init();

    let center = Coord::from((22.8996, -19.3325));

    let mut client = Client::connect("http://127.0.0.1:9100").await?;

    let play_db = "play";
    client
        .execute_query_checked(QueryInfo {
            query: format!("create database if not exists {}", play_db),
            ..Default::default()
        })
        .await?;

    let schema = okavango_delta_schema()?;
    client.drop_tableset(play_db, &schema.name).await?;
    //return Ok(());
    client.create_tableset(play_db, &schema).await?;

    let tablesets = client.list_tablesets(play_db).await?;
    assert!(tablesets.contains_key(&schema.name));

    let h3df = make_h3dataframe(center)?;
    //dbg!(h3df.dataframe().shape());

    client
        .insert_h3dataframe_into_tableset(
            play_db,
            &schema,
            h3df,
            InsertOptions {
                max_num_rows_per_chunk: 20,
                ..Default::default()
            },
        )
        .await?;

    dbg!(client.tableset_stats(play_db, &schema.name).await?);

    let queried_df = client
        .query_tableset_cells(
            play_db,
            &schema.name,
            QueryOptions::new(
                Default::default(),
                vec![H3Cell::from_coordinate(center, MAX_H3_RES - 1)?],
                MAX_H3_RES,
            ),
        )
        .await?;
    assert_eq!(queried_df.dataframe().shape().0, 7);

    client.drop_tableset(play_db, "okavango_delta").await?;
    assert!(!client
        .list_tablesets(play_db)
        .await?
        .contains_key("okavango_delta"));

    Ok(())
}

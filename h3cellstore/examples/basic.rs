use h3cellstore::clickhouse::clickhouse_arrow_grpc::{ArrowInterface, ClickHouseClient, QueryInfo};
use h3cellstore::clickhouse::compacted_tables::schema::{
    AggregationMethod, ClickhouseDataType, ColumnDefinition, CompactedTableSchema,
    CompactedTableSchemaBuilder, SimpleColumn, TemporalPartitioning,
};
use h3cellstore::clickhouse::compacted_tables::CompactedTablesStore;

fn okavango_delta_schema() -> eyre::Result<CompactedTableSchema> {
    let schema = CompactedTableSchemaBuilder::new("okavango_delta")
        .h3_base_resolutions(vec![1, 2, 3, 4, 5])
        .temporal_partitioning(TemporalPartitioning::Month)
        .add_column(
            "elephant_density",
            ColumnDefinition::WithAggregation(
                SimpleColumn::new(ClickhouseDataType::Float32, None),
                AggregationMethod::Average,
            ),
        )
        .add_column(
            "observed_on",
            ColumnDefinition::Simple(SimpleColumn::new(ClickhouseDataType::DateTime64, Some(0))),
        )
        .build()?;
    Ok(schema)
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt::init();

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
    client.create_tableset_schema(&play_db, &schema).await?;

    let tablesets = client.list_tablesets(&play_db).await?;
    assert!(tablesets.contains_key("okavango_delta"));

    Ok(())
}

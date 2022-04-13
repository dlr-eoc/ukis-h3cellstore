use polars_core::frame::DataFrame;
use tokio::task::spawn_blocking;
use tonic::transport::Channel;

use crate::api::click_house_client::ClickHouseClient;
use crate::api::{QueryInfo, Result as QueryResult};
use crate::arrow_integration::serialize_for_clickhouse;

pub mod api;
mod arrow_integration;
mod error;

pub use self::error::Error;

pub async fn query(
    client: &mut ClickHouseClient<Channel>,
    q: QueryInfo,
) -> Result<QueryResult, Error> {
    dbg!(&q.query);
    let response = client.execute_query(q).await?.into_inner();
    match response.exception {
        Some(ex) => Err(Error::ClickhouseException {
            name: ex.name,
            display_text: ex.display_text,
            stack_trace: ex.stack_trace,
        }),
        None => Ok(response),
    }
}

pub async fn query_to_dataframe(
    client: &mut ClickHouseClient<Channel>,
    mut q: QueryInfo,
) -> Result<DataFrame, Error> {
    q.output_format = "Arrow".to_string();
    q.send_output_columns = true;
    let response = query(client, q).await?;
    spawn_blocking(move || response.try_into()).await?
}

pub async fn insert_dataframe(
    client: &mut ClickHouseClient<Channel>,
    database_name: &str,
    table_name: &str,
    mut df: DataFrame,
) -> Result<(), Error> {
    let input_data = spawn_blocking(move || serialize_for_clickhouse(&mut df)).await??;
    let q = QueryInfo {
        query: format!("insert into {} FORMAT Arrow", table_name),
        database: database_name.to_string(),
        input_data,
        ..Default::default()
    };
    query(client, q).await?;
    Ok(())
}

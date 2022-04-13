use async_trait::async_trait;
use polars_core::frame::DataFrame;
use tokio::task::spawn_blocking;
use tonic::transport::Channel;

pub use crate::api::click_house_client::ClickHouseClient;
pub use crate::api::{QueryInfo, Result as QueryResult};
use crate::arrow_integration::serialize_for_clickhouse;

pub use self::error::Error;

pub mod api;
mod arrow_integration;
mod error;

#[async_trait]
trait ArrowInterface {
    async fn execute_query_checked(&mut self, q: QueryInfo) -> Result<QueryResult, Error>;

    async fn query_into_dataframe(&mut self, mut q: QueryInfo) -> Result<DataFrame, Error>;

    async fn insert_dataframe(
        &mut self,
        database_name: &str,
        table_name: &str,
        mut df: DataFrame,
    ) -> Result<(), Error>;
}

#[async_trait]
impl ArrowInterface for ClickHouseClient<Channel> {
    async fn execute_query_checked(&mut self, q: QueryInfo) -> Result<QueryResult, Error> {
        dbg!(&q.query);
        let response = self.execute_query(q).await?.into_inner();

        match response.exception {
            Some(ex) => Err(Error::ClickhouseException {
                name: ex.name,
                display_text: ex.display_text,
                stack_trace: ex.stack_trace,
            }),
            None => Ok(response),
        }
    }

    async fn query_into_dataframe(&mut self, mut q: QueryInfo) -> Result<DataFrame, Error> {
        q.output_format = "Arrow".to_string();
        q.send_output_columns = true;
        let response = self.execute_query_checked(q).await?;
        spawn_blocking(move || response.try_into()).await?
    }

    async fn insert_dataframe(
        &mut self,
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
        self.execute_query_checked(q).await?;
        Ok(())
    }
}

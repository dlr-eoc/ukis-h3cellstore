use async_trait::async_trait;
use polars_core::frame::DataFrame;
use tokio::task::spawn_blocking;
use tonic::transport::Channel;
use tracing::{span, Instrument, Level};

pub use crate::api::click_house_client::ClickHouseClient;
pub use crate::api::{QueryInfo, Result as QueryResult};
use crate::arrow_integration::serialize_for_clickhouse;
#[cfg(feature = "reexport-deps")]
pub use arrow2;
#[cfg(feature = "reexport-deps")]
pub use tokio;
#[cfg(feature = "reexport-deps")]
pub use tonic; // for downstream dependency management

pub use self::error::Error;

pub mod api;
mod arrow_integration;
mod error;

#[async_trait]
pub trait ArrowInterface {
    /// execute the query, check the response for errors and return as a rust `Result` type.
    async fn execute_query_checked(&mut self, q: QueryInfo) -> Result<QueryResult, Error>;

    async fn execute_into_dataframe(&mut self, mut q: QueryInfo) -> Result<DataFrame, Error>;

    async fn insert_dataframe<S1, S2>(
        &mut self,
        database_name: S1,
        table_name: S2,
        mut df: DataFrame,
    ) -> Result<(), Error>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send;
}

#[async_trait]
impl ArrowInterface for ClickHouseClient<Channel> {
    async fn execute_query_checked(&mut self, q: QueryInfo) -> Result<QueryResult, Error> {
        let span = span!(Level::DEBUG, "Executing query", query = q.query.as_str());

        let response = self.execute_query(q).instrument(span).await?.into_inner();

        match response.exception {
            Some(ex) => Err(Error::ClickhouseException {
                name: ex.name,
                display_text: ex.display_text,
                stack_trace: ex.stack_trace,
            }),
            None => Ok(response),
        }
    }

    async fn execute_into_dataframe(&mut self, mut q: QueryInfo) -> Result<DataFrame, Error> {
        q.output_format = "Arrow".to_string();
        q.send_output_columns = true;
        let response = self.execute_query_checked(q).await?;
        spawn_blocking(move || response.try_into()).await?
    }

    async fn insert_dataframe<S1, S2>(
        &mut self,
        database_name: S1,
        table_name: S2,
        mut df: DataFrame,
    ) -> Result<(), Error>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
    {
        let input_data = spawn_blocking(move || serialize_for_clickhouse(&mut df)).await??;
        let q = QueryInfo {
            query: format!("insert into {} FORMAT Arrow", table_name.as_ref()),
            database: database_name.as_ref().to_string(),
            input_data,
            ..Default::default()
        };
        self.execute_query_checked(q).await?;
        Ok(())
    }
}

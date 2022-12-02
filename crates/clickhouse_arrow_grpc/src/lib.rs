use async_trait::async_trait;
use polars_core::frame::DataFrame;
use std::ops::{Deref, DerefMut};
use tokio::task::spawn_blocking;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tracing::{span, Instrument, Level};

use crate::api::click_house_client::ClickHouseClient;
pub use crate::api::{QueryInfo, Result as QueryResult};
use crate::arrow_integration::serialize_for_clickhouse;

pub use self::error::Error;

// for downstream dependency management

pub mod api;
mod arrow_integration;
mod error;
pub mod export;

/// Client.
///
/// Pre-configures the underlying gprc service to use transport compression
#[derive(Clone, Debug)]
pub struct Client(ClickHouseClient<Channel>);

impl Client {
    pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
    where
        D: TryInto<tonic::transport::Endpoint>,
        D::Error: Into<tonic::codegen::StdError>,
    {
        let channel = tonic::transport::Endpoint::new(dst)?.connect().await?;
        let cc = ClickHouseClient::new(channel);
        Ok(cc.into())
    }

    fn preconfigure_queryinfo(&self, query_info: &mut QueryInfo) {
        query_info.transport_compression_type = "gzip".to_string()
    }
}

impl From<ClickHouseClient<Channel>> for Client {
    fn from(cc: ClickHouseClient<Channel>) -> Self {
        let cc = cc
            .accept_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Gzip);

        Self(cc)
    }
}

impl Deref for Client {
    type Target = ClickHouseClient<Channel>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Client {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Client> for ClickHouseClient<Channel> {
    fn from(c: Client) -> Self {
        c.0
    }
}

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
impl ArrowInterface for Client {
    async fn execute_query_checked(&mut self, mut q: QueryInfo) -> Result<QueryResult, Error> {
        let span = span!(
            Level::DEBUG,
            "Executing checked query",
            query = q.query.as_str()
        );
        self.preconfigure_queryinfo(&mut q);

        let response = self.execute_query(q).instrument(span).await?.into_inner();

        match response.exception {
            Some(ex) => Err(Error::ClickhouseException(ClickhouseException {
                name: ex.name,
                display_text: ex.display_text,
                stack_trace: ex.stack_trace,
            })),
            None => Ok(response),
        }
    }

    async fn execute_into_dataframe(&mut self, mut q: QueryInfo) -> Result<DataFrame, Error> {
        q.output_format = "Arrow".to_string();
        q.send_output_columns = true;
        self.preconfigure_queryinfo(&mut q);
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
        let mut q = QueryInfo {
            query: format!("insert into {} FORMAT Arrow", table_name.as_ref()),
            database: database_name.as_ref().to_string(),
            input_data,
            ..Default::default()
        };
        self.preconfigure_queryinfo(&mut q);
        self.execute_query_checked(q).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct ClickhouseException {
    pub name: String,
    pub display_text: String,
    pub stack_trace: String,
}

impl ToString for ClickhouseException {
    fn to_string(&self) -> String {
        format!("{}: {}", self.name, self.display_text)
    }
}

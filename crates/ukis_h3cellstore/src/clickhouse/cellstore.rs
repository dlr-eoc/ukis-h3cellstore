use async_trait::async_trait;
use h3ron::H3Cell;
use h3ron_polars::frame::H3DataFrame;

use ukis_clickhouse_arrow_grpc::{ArrowInterface, QueryInfo};

use crate::Error;

#[async_trait]
pub trait H3CellStore {
    async fn execute_into_h3dataframe<S>(
        &mut self,
        mut q: QueryInfo,
        h3index_column_name: S,
    ) -> Result<H3DataFrame<H3Cell>, Error>
    where
        S: AsRef<str> + Send;

    async fn insert_h3dataframe<S1, S2>(
        &mut self,

        database_name: S1,
        table_name: S2,
        mut h3df: H3DataFrame<H3Cell>,
    ) -> Result<(), Error>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send;

    async fn insert_h3dataframe_chunked<S1, S2>(
        &mut self,

        database_name: S1,
        table_name: S2,
        h3df: H3DataFrame<H3Cell>,
        max_num_rows_per_chunk: usize,
    ) -> Result<(), Error>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
    {
        let db_name = database_name.as_ref().to_string();
        let tb_name = table_name.as_ref().to_string();

        let mut current_offset = 0i64;
        while current_offset < h3df.dataframe().shape().0 as i64 {
            let chunk_h3df = H3DataFrame::from_dataframe_nonvalidated(
                h3df.dataframe()
                    .slice(current_offset, max_num_rows_per_chunk),
                h3df.h3index_column_name(),
            );
            current_offset += max_num_rows_per_chunk as i64;
            self.insert_h3dataframe(&db_name, &tb_name, chunk_h3df)
                .await?;
        }
        Ok(())
    }

    async fn database_exists<S>(&mut self, database_name: S) -> Result<bool, Error>
    where
        S: AsRef<str> + Send;
}

#[async_trait]
impl<C> H3CellStore for C
where
    C: ArrowInterface + Send,
{
    async fn execute_into_h3dataframe<S>(
        &mut self,
        q: QueryInfo,
        h3index_column_name: S,
    ) -> Result<H3DataFrame<H3Cell>, Error>
    where
        S: AsRef<str> + Send,
    {
        let df = self.execute_into_dataframe(q).await?;
        Ok(H3DataFrame::from_dataframe(df, h3index_column_name)?)
    }

    async fn insert_h3dataframe<S1, S2>(
        &mut self,
        database_name: S1,
        table_name: S2,
        h3df: H3DataFrame<H3Cell>,
    ) -> Result<(), Error>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
    {
        let df = h3df.into_dataframe();
        Ok(self.insert_dataframe(database_name, table_name, df).await?)
    }

    async fn database_exists<S>(&mut self, database_name: S) -> Result<bool, Error>
    where
        S: AsRef<str> + Send,
    {
        let df = self
            .execute_into_dataframe(QueryInfo {
                query: format!(
                    "select name from databases where name = '{}'",
                    database_name.as_ref()
                ),
                database: "system".to_string(),
                ..Default::default()
            })
            .await?;
        Ok(df.shape().0 != 0)
    }
}

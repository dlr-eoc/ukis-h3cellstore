use crate::Error;
use arrow_h3::algo::IterRowCountLimited;
use arrow_h3::H3DataFrame;
use async_trait::async_trait;
use clickhouse_arrow_grpc::ArrowInterface;
use clickhouse_arrow_grpc::QueryInfo;

#[async_trait]
pub trait H3CellStore {
    async fn execute_into_h3dataframe<S>(
        &mut self,
        mut q: QueryInfo,
        h3index_column_name: S,
    ) -> Result<H3DataFrame, Error>
    where
        S: AsRef<str> + Send;

    async fn insert_h3dataframe<S1, S2>(
        &mut self,

        database_name: S1,
        table_name: S2,
        mut h3df: H3DataFrame,
    ) -> Result<(), Error>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send;

    async fn insert_h3dataframe_chunked<S1, S2>(
        &mut self,

        database_name: S1,
        table_name: S2,
        h3df: H3DataFrame,
        max_num_rows_per_chunk: usize,
    ) -> Result<(), Error>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
    {
        let db_name = database_name.as_ref().to_string();
        let tb_name = table_name.as_ref().to_string();
        for chunk_h3df in h3df.iter_row_count_limited(max_num_rows_per_chunk)? {
            self.insert_h3dataframe(&db_name, &tb_name, chunk_h3df)
                .await?;
        }
        Ok(())
    }
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
    ) -> Result<H3DataFrame, Error>
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
        h3df: H3DataFrame,
    ) -> Result<(), Error>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
    {
        Ok(self
            .insert_dataframe(database_name, table_name, h3df.dataframe)
            .await?)
    }
}

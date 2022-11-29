use crate::clickhouse::compacted_tables::traversal::{
    traverse, TraversalArea, TraversalOptions, TraversedCell, Traverser,
};
use crate::clickhouse::H3CellStore;
use crate::Error;
use clickhouse_arrow_grpc::export::tonic::codec::CompressionEncoding;
use clickhouse_arrow_grpc::export::tonic::codegen::futures_core::Stream;
use clickhouse_arrow_grpc::export::tonic::transport::Channel;
use clickhouse_arrow_grpc::ClickHouseClient;
use futures::StreamExt;
use std::sync::Arc;
use tokio::runtime::Runtime;

/// Synchronous Bridge to some of the async h3cellstore APIs
pub struct SyncBridge {
    rt: Arc<Runtime>,
    database_name: String,
    client: ClickHouseClient<Channel>,
}

impl SyncBridge {
    pub fn connect_grpc<EP, DBN>(endpoint: EP, database_name: DBN) -> Result<Self, Error>
    where
        EP: AsRef<str>,
        DBN: AsRef<str>,
    {
        let rt = Arc::new(build_runtime()?);
        Self::connect_grpc_with_runtime(rt, endpoint, database_name)
    }

    ///
    /// The `runtime` should be a multithreaded variant
    pub fn connect_grpc_with_runtime<EP, DBN>(
        runtime: Arc<Runtime>,
        endpoint: EP,
        database_name: DBN,
    ) -> Result<Self, Error>
    where
        EP: AsRef<str>,
        DBN: AsRef<str>,
    {
        let endpoint_string = endpoint.as_ref().to_string();
        let mut client = runtime.block_on(async {
            ClickHouseClient::connect(endpoint_string)
                .await
                .map(|client| {
                    client
                        .accept_compressed(CompressionEncoding::Gzip)
                        .send_compressed(CompressionEncoding::Gzip)
                })
        })?;

        let database_name = database_name.as_ref().to_string();
        if !runtime.block_on(async { client.database_exists(&database_name).await })? {
            return Err(Error::DatabaseNotFound(database_name));
        }

        Ok(Self {
            rt: runtime,
            database_name,
            client,
        })
    }

    pub fn traverse<TSN>(
        &self,
        tableset_name: TSN,
        area: &TraversalArea,
        options: TraversalOptions,
    ) -> Result<TraverseIterator, Error>
    where
        TSN: AsRef<str>,
    {
        let tableset_name = tableset_name.as_ref().to_string();
        let mut client2 = self.client.clone();
        let traverser = self.rt.block_on(async {
            traverse(
                &mut client2,
                self.database_name.clone(),
                tableset_name,
                area,
                options,
            )
            .await
        })?;
        Ok(TraverseIterator {
            rt: self.rt.clone(),
            traverser,
        })
    }
}

pub struct TraverseIterator {
    rt: Arc<Runtime>,
    traverser: Traverser,
}

impl Iterator for TraverseIterator {
    type Item = Result<TraversedCell, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rt.block_on(async { self.traverser.next().await })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.traverser.size_hint()
    }
}

fn build_runtime() -> Result<Runtime, Error> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(3);
    let runtime = builder.enable_all().build()?;
    Ok(runtime)
}

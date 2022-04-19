use async_trait::async_trait;

#[async_trait]
pub trait CompactedTablesStore {
    async fn list_tablesets(&mut self);
}

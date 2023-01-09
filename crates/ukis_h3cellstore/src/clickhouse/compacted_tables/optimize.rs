use itertools::Itertools;
use tracing::{debug_span, Instrument};

use ukis_clickhouse_arrow_grpc::{ArrowInterface, QueryInfo};

use crate::clickhouse::compacted_tables::schema::{CompactedTableSchema, ResolutionMetadata};
use crate::clickhouse::compacted_tables::temporary_key::TemporaryKey;
use crate::Error;

// OLD MEMO:
//
// this could also be implemented by obtaining the partition expression from
// the clickhouse `system.parts` using a query like this one:
//
// `select name, partition_key from system.tables where name = 'timestamp_test_04_base';`
//
// that solution would be more resilient in case the schema description in this library
// has diverged from the database tables.

pub(crate) async fn deduplicate_partitions_based_on_temporary_tables<C, S>(
    store: &mut C,
    database_name: S,
    schema: &CompactedTableSchema,
    resolution_metadata_slice: &[ResolutionMetadata],
    temporary_key: &Option<TemporaryKey>,
) -> Result<(), Error>
where
    C: ArrowInterface + Send + Sync,
    S: AsRef<str> + Send + Sync,
{
    let part_expr = schema.partition_by_expressions()?;
    if !part_expr.is_empty() && temporary_key.is_some() {
        let part_expr_string = part_expr.iter().join(", ");
        for resolution_metadata in resolution_metadata_slice.iter() {
            let table_temp = schema
                .build_table(resolution_metadata, temporary_key)
                .to_table_name();

            // obtain the list of relevant partitions which did receive changes by running
            // the partition expression on the temporary table.
            let partitions: Vec<_> = store
                .execute_into_dataframe(QueryInfo {
                    query: format!(
                        "select distinct toString(({})) pe from {}",
                        part_expr_string, table_temp
                    ),
                    database: database_name.as_ref().to_string(),
                    ..Default::default()
                })
                .await?
                .column("pe")?
                .utf8()?
                .into_iter()
                .flatten()
                .map(str::to_string)
                .collect();

            let table_final = schema
                .build_table(resolution_metadata, &None)
                .to_table_name();
            for partition in partitions.iter() {
                store
                    .execute_query_checked(QueryInfo {
                        query: format!(
                            "optimize table {} partition {} deduplicate",
                            table_final, partition
                        ),
                        database: database_name.as_ref().to_string(),
                        ..Default::default()
                    })
                    .instrument(debug_span!(
                        "De-duplicating partition of table",
                        table_name = table_final.as_str(),
                        partition = partition.as_str()
                    ))
                    .await?;
            }
        }
        Ok(())
    } else {
        Err(Error::MissingPrecondidtionsForPartialOptimization)
    }
}

/// without a partitioning expression we got to deduplicate all partitions
pub(crate) async fn deduplicate_full<C, S>(
    store: &mut C,
    database_name: S,
    schema: &CompactedTableSchema,
    resolution_metadata_slice: &[ResolutionMetadata],
) -> Result<(), Error>
where
    C: ArrowInterface + Send + Sync,
    S: AsRef<str> + Send + Sync,
{
    for resolution_metadata in resolution_metadata_slice.iter() {
        let table_final = schema
            .build_table(resolution_metadata, &None)
            .to_table_name();
        store
            .execute_query_checked(QueryInfo {
                query: format!("optimize table {} deduplicate", table_final),
                database: database_name.as_ref().to_string(),
                ..Default::default()
            })
            .instrument(debug_span!(
                "De-duplicating complete table",
                table_name = table_final.as_str()
            ))
            .await?;
    }
    Ok(())
}

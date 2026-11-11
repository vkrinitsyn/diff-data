use chrono::NaiveDateTime;
use tokio_postgres::Client;
use tracing::debug;

use crate::config::parse_table_name;
use crate::error::{DataDiffError, Result};
use crate::sql_gen::{format_table_name, quote_identifier};
use crate::types::{ChunkStrategy, ColumnInfo};

/// Detect a timestamp column for chunking.
///
/// Priority:
/// 1. Config override (`config_timestamp`)
/// 2. Column matching `update*` pattern (TIMESTAMP/TIMESTAMPTZ/DATE type)
/// 3. Column matching `create*` pattern (TIMESTAMP/TIMESTAMPTZ/DATE type)
pub fn detect_timestamp_column(
    columns: &[ColumnInfo],
    config_timestamp: Option<&str>,
) -> Option<String> {
    if let Some(ts) = config_timestamp {
        // Verify the column exists
        if columns.iter().any(|c| c.name.eq_ignore_ascii_case(ts)) {
            return Some(ts.to_string());
        }
    }

    let ts_types = ["timestamp", "timestamptz", "date"];

    // Look for update* columns
    for col in columns {
        let type_name = format!("{}", col.pg_type);
        let lower_name = col.name.to_lowercase();
        if lower_name.starts_with("update")
            && ts_types.iter().any(|t| type_name.to_lowercase().contains(t))
        {
            return Some(col.name.clone());
        }
    }

    // Look for create* columns
    for col in columns {
        let type_name = format!("{}", col.pg_type);
        let lower_name = col.name.to_lowercase();
        if lower_name.starts_with("create")
            && ts_types.iter().any(|t| type_name.to_lowercase().contains(t))
        {
            return Some(col.name.clone());
        }
    }

    None
}

/// Get an approximate row count for a table using pg_class.reltuples.
pub async fn get_row_count(client: &Client, schema_qualified_name: &str) -> Result<i64> {
    let (schema_opt, table_name) = parse_table_name(schema_qualified_name);
    let schema = if let Some(s) = schema_opt {
        s.to_string()
    } else {
        let row = client.query_one("SELECT current_schema()", &[]).await?;
        row.get(0)
    };

    let row = client
        .query_one(
            "SELECT COALESCE(reltuples::bigint, 0) FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname = $2",
            &[&schema, &table_name],
        )
        .await?;

    Ok(row.get::<_, i64>(0))
}

/// Get the min and max values of a timestamp column.
pub async fn get_timestamp_range(
    client: &Client,
    schema_qualified_name: &str,
    ts_col: &str,
) -> Result<Option<(NaiveDateTime, NaiveDateTime)>> {
    let table_sql = format_table_name(schema_qualified_name);
    let col_sql = quote_identifier(ts_col);

    let sql = format!(
        "SELECT MIN({col})::timestamp, MAX({col})::timestamp FROM {tbl}",
        col = col_sql,
        tbl = table_sql,
    );

    let row = client.query_one(&sql, &[]).await?;

    let min_ts: Option<NaiveDateTime> = row.get(0);
    let max_ts: Option<NaiveDateTime> = row.get(1);

    match (min_ts, max_ts) {
        (Some(min), Some(max)) => Ok(Some((min, max))),
        _ => Ok(None),
    }
}

/// Determine the chunk strategy for a table based on row count and available timestamp column.
///
/// Returns `(strategy, detected_timestamp_column)`.
///
/// Logic:
/// - >1M rows + timestamp → `Md5TimestampChunk`
/// - >1M rows + no timestamp + excluded → `IdChunkLargeExcluded`
/// - >1M rows + no timestamp + not excluded → Error
/// - 100K-1M rows → `IdChunkMedium`
/// - <100K rows → `IdChunkSmall`
pub async fn determine_strategy(
    client: &Client,
    schema_qualified_name: &str,
    columns: &[ColumnInfo],
    config_timestamp: Option<&str>,
    is_excluded: bool,
) -> Result<(ChunkStrategy, Option<String>)> {
    let row_count = get_row_count(client, schema_qualified_name).await?;
    debug!(
        "Table {} row count: {}",
        schema_qualified_name, row_count
    );

    if row_count < 100_000 {
        return Ok((ChunkStrategy::IdChunkSmall, None));
    }

    if row_count < 1_000_000 {
        let ts_col = detect_timestamp_column(columns, config_timestamp);
        return Ok((ChunkStrategy::IdChunkMedium, ts_col));
    }

    // >1M rows
    let ts_col = detect_timestamp_column(columns, config_timestamp);

    match ts_col {
        Some(ref col) => {
            let range = get_timestamp_range(client, schema_qualified_name, col).await?;
            match range {
                Some((min_ts, max_ts)) => {
                    let duration = max_ts.signed_duration_since(min_ts);
                    let total_days = duration.num_seconds() as f64 / 86400.0;
                    let chunk_days = if total_days > 0.0 {
                        total_days / 10.0
                    } else {
                        1.0
                    };

                    Ok((
                        ChunkStrategy::Md5TimestampChunk {
                            timestamp_column: col.clone(),
                            min_ts,
                            max_ts,
                            chunk_days,
                        },
                        Some(col.clone()),
                    ))
                }
                None => {
                    // No data in the table, treat as small
                    Ok((ChunkStrategy::IdChunkSmall, Some(col.clone())))
                }
            }
        }
        None => {
            if is_excluded {
                Ok((ChunkStrategy::IdChunkLargeExcluded, None))
            } else {
                Err(DataDiffError::Config(tr!("error.large_no_timestamp", table = schema_qualified_name)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use postgres_types::Type;

    fn col(name: &str, pg_type: Type) -> ColumnInfo {
        ColumnInfo { name: name.to_string(), pg_type, is_pk: false, is_identity: false, identity_always: false, ordinal: 0 }
    }

    #[test]
    fn the_timestamp_column_is_the_configured_one_then_update_then_create() {
        let cols = vec![
            col("id", Type::INT4),
            col("updated_by", Type::TEXT),
            col("Created_On", Type::DATE),
            col("UpdatedAt", Type::TIMESTAMPTZ),
            col("seen", Type::TIMESTAMP),
        ];
        assert_eq!(detect_timestamp_column(&cols, Some("SEEN")), Some("SEEN".to_string()));
        // a configured column the table lacks falls back to detection
        assert_eq!(detect_timestamp_column(&cols, Some("missing")), Some("UpdatedAt".to_string()));
        assert_eq!(detect_timestamp_column(&cols[..3], None), Some("Created_On".to_string()));
        assert_eq!(detect_timestamp_column(&cols[..2], None), None);
    }
}

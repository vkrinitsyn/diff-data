use std::fmt;

use postgres_types::Type;
use serde::{Deserialize, Serialize};

/// Chunk strategy determined by table size and available timestamp column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChunkStrategy {
    /// Small table (<100K rows): chunk by ID.
    IdChunkSmall,
    /// Medium table (100K-1M rows): chunk by ID.
    IdChunkMedium,
    /// Large table (>1M rows) with timestamp column: MD5 hash comparison on time-based chunks.
    Md5TimestampChunk {
        timestamp_column: String,
        min_ts: chrono::NaiveDateTime,
        max_ts: chrono::NaiveDateTime,
        chunk_days: f64,
    },
    /// Large table (>1M rows) without timestamp, but excluded from strict comparison.
    IdChunkLargeExcluded,
}

impl fmt::Display for ChunkStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChunkStrategy::IdChunkSmall => write!(f, "IdChunkSmall"),
            ChunkStrategy::IdChunkMedium => write!(f, "IdChunkMedium"),
            ChunkStrategy::Md5TimestampChunk { timestamp_column, chunk_days, .. } => {
                write!(f, "Md5TimestampChunk(col={}, chunk_days={:.1})", timestamp_column, chunk_days)
            }
            ChunkStrategy::IdChunkLargeExcluded => write!(f, "IdChunkLargeExcluded"),
        }
    }
}

/// Analysis result for a single table, including the chosen chunk strategy and ETC estimate.
#[derive(Debug, Clone)]
pub struct TableAnalysis {
    pub ref_table: String,
    pub target_table: String,
    pub row_count: i64,
    pub timestamp_column: Option<String>,
    pub strategy: ChunkStrategy,
    pub estimated_seconds: f64,
}

/// Dynamic value extracted from a PostgreSQL row.
#[derive(Debug, Clone, PartialEq)]
pub enum RowValue {
    Null,
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Text(String),
    Bytes(Vec<u8>),
    Timestamp(chrono::NaiveDateTime),
    TimestampTz(chrono::DateTime<chrono::Utc>),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
    Uuid(uuid::Uuid),
    Json(serde_json::Value),
    /// NUMERIC/DECIMAL stored as string to avoid precision loss.
    Numeric(String),
}

/// Metadata for a single column.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub pg_type: Type,
    pub is_pk: bool,
    /// an identity column (or a serial: its default a `nextval`)
    pub is_identity: bool,
    /// `GENERATED ALWAYS AS IDENTITY`: an INSERT that writes it says `OVERRIDING SYSTEM VALUE`
    pub identity_always: bool,
    /// Ordinal position (0-based) in the table's column list.
    pub ordinal: usize,
}

/// Result of attempting to set up a table pair for comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableDiffStatus {
    Ok,
    ColumnMismatch,
    ReferenceNotFound,
    TargetNotFound,
    NoPrimaryKey,
}

/// A row is a vector of column values in ordinal order.
pub type Row = Vec<RowValue>;

/// Statistics for the INSERT/UPDATE phase.
#[derive(Debug, Clone, Default)]
pub struct DiffStats {
    pub inserts: u64,
    pub updates: u64,
}

/// Statistics for a single table's diff.
#[derive(Debug, Clone, Default)]
pub struct TableDiffReport {
    pub table_name: String,
    pub inserts: u64,
    pub updates: u64,
    pub deletes: u64,
    pub insert_file: Option<String>,
    pub update_file: Option<String>,
    pub delete_file: Option<String>,
}

/// Overall diff report.
#[derive(Debug, Clone, Default)]
pub struct DiffReport {
    pub tables: Vec<TableDiffReport>,
    pub warnings: Vec<String>,
    pub master_script: Option<String>,
}

/// Extract a `RowValue` from a `tokio_postgres::Row` at a given column index
/// based on the column's PostgreSQL type.
pub fn extract_row_value(row: &tokio_postgres::Row, idx: usize, pg_type: &Type) -> RowValue {
    // Check for NULL first via try_get with Option
    match *pg_type {
        Type::BOOL => match row.try_get::<_, Option<bool>>(idx) {
            Ok(Some(v)) => RowValue::Bool(v),
            _ => RowValue::Null,
        },
        Type::INT2 => match row.try_get::<_, Option<i16>>(idx) {
            Ok(Some(v)) => RowValue::I16(v),
            _ => RowValue::Null,
        },
        Type::INT4 => match row.try_get::<_, Option<i32>>(idx) {
            Ok(Some(v)) => RowValue::I32(v),
            _ => RowValue::Null,
        },
        // an OID is unsigned: past i32::MAX it fits an i64 only
        Type::OID => match row.try_get::<_, Option<u32>>(idx) {
            Ok(Some(v)) => RowValue::I64(v.into()),
            _ => RowValue::Null,
        },
        Type::INT8 => match row.try_get::<_, Option<i64>>(idx) {
            Ok(Some(v)) => RowValue::I64(v),
            _ => RowValue::Null,
        },
        Type::FLOAT4 => match row.try_get::<_, Option<f32>>(idx) {
            Ok(Some(v)) => RowValue::F32(v),
            _ => RowValue::Null,
        },
        Type::FLOAT8 => match row.try_get::<_, Option<f64>>(idx) {
            Ok(Some(v)) => RowValue::F64(v),
            _ => RowValue::Null,
        },
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => {
            match row.try_get::<_, Option<String>>(idx) {
                Ok(Some(v)) => RowValue::Text(v),
                _ => RowValue::Null,
            }
        }
        Type::BYTEA => match row.try_get::<_, Option<Vec<u8>>>(idx) {
            Ok(Some(v)) => RowValue::Bytes(v),
            _ => RowValue::Null,
        },
        Type::TIMESTAMP => match row.try_get::<_, Option<chrono::NaiveDateTime>>(idx) {
            Ok(Some(v)) => RowValue::Timestamp(v),
            _ => RowValue::Null,
        },
        Type::TIMESTAMPTZ => {
            match row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(idx) {
                Ok(Some(v)) => RowValue::TimestampTz(v),
                _ => RowValue::Null,
            }
        }
        Type::DATE => match row.try_get::<_, Option<chrono::NaiveDate>>(idx) {
            Ok(Some(v)) => RowValue::Date(v),
            _ => RowValue::Null,
        },
        Type::TIME => match row.try_get::<_, Option<chrono::NaiveTime>>(idx) {
            Ok(Some(v)) => RowValue::Time(v),
            _ => RowValue::Null,
        },
        Type::UUID => match row.try_get::<_, Option<uuid::Uuid>>(idx) {
            Ok(Some(v)) => RowValue::Uuid(v),
            _ => RowValue::Null,
        },
        Type::JSON | Type::JSONB => match row.try_get::<_, Option<serde_json::Value>>(idx) {
            Ok(Some(v)) => RowValue::Json(v),
            _ => RowValue::Null,
        },
        Type::NUMERIC => {
            // NUMERIC is read as its exact text, which the diff's queries select
            // (`sql_gen::select_column` casts it): no precision is lost.
            match row.try_get::<_, Option<String>>(idx) {
                Ok(Some(v)) => RowValue::Numeric(v),
                _ => RowValue::Null,
            }
        }
        // Fallback: read as text (the diff's queries cast what it does not decode to text)
        _ => match row.try_get::<_, Option<String>>(idx) {
            Ok(Some(v)) => RowValue::Text(v),
            _ => RowValue::Null,
        },
    }
}

/// The key of a row as one string: `n` text columns from `start` (selected by `sql_gen::pk_text_list`),
/// joined by `'|'`; a NULL one as `NULL`.
pub fn text_key(row: &tokio_postgres::Row, start: usize, n: usize) -> String {
    (start..start + n)
        .map(|i| row.try_get::<_, Option<String>>(i).ok().flatten().unwrap_or_else(|| "NULL".to_string()))
        .collect::<Vec<_>>()
        .join("|")
}

/// Extract an entire row into a `Vec<RowValue>` given column info.
pub fn extract_row(row: &tokio_postgres::Row, columns: &[ColumnInfo]) -> Row {
    columns
        .iter()
        .enumerate()
        .map(|(idx, col)| extract_row_value(row, idx, &col.pg_type))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_strategy_displays_its_name_and_its_parameters() {
        let ts = chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap();
        assert_eq!(ChunkStrategy::IdChunkSmall.to_string(), "IdChunkSmall");
        assert_eq!(ChunkStrategy::IdChunkMedium.to_string(), "IdChunkMedium");
        assert_eq!(ChunkStrategy::IdChunkLargeExcluded.to_string(), "IdChunkLargeExcluded");
        let md5 = ChunkStrategy::Md5TimestampChunk {
            timestamp_column: "updated_at".into(),
            min_ts: ts,
            max_ts: ts,
            chunk_days: 2.54,
        };
        assert_eq!(md5.to_string(), "Md5TimestampChunk(col=updated_at, chunk_days=2.5)");
    }
}

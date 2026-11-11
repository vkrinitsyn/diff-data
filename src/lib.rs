//! diff_data (data-diff): no unsafe code - the compiler holds it to that
#![forbid(unsafe_code)]

pub mod checksum;
pub mod chunk_strategy;
/// N copies of a table across a cluster: YT Cluster's compares (ytserv's recon first, ytctl's data diff second)
pub mod cluster;
pub mod config;
pub mod data_diff;
pub mod delete_sync;
pub mod error;
pub mod estimation;
pub mod metadata;
pub mod schema_compare;
pub mod sql_gen;
pub mod table_diff;
pub mod types;

pub use checksum::{
    ChecksumManifest, ChunkChecksum, TableChecksum, TableDiffSummary,
    compare_checksums, compute_db_checksums, compute_table_checksums,
    load_checksums, save_checksums,
};
pub use config::{DiffConfig, PgPool, TablePair};
pub use data_diff::DataDiff;
pub use error::{DataDiffError, Result};
pub use types::{
    ChunkStrategy, DiffReport, DiffStats, SchemaComparisonResult, TableAnalysis,
    TableDiffReport, TableDiffStatus,
};

use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::NoTls;

/// Convenience function to create a bb8 connection pool from a PostgreSQL connection string.
pub async fn create_pool(connection_string: &str) -> Result<PgPool> {
    let manager = PostgresConnectionManager::new_from_stringlike(connection_string, NoTls)
        .map_err(|e| DataDiffError::Config(format!("Invalid connection string: {}", e)))?;

    let pool = bb8::Pool::builder()
        .max_size(4)
        .build(manager)
        .await
        .map_err(|e| DataDiffError::Config(format!("Failed to create pool: {}", e)))?;

    Ok(pool)
}

/// Convenience function to run a data diff with a config.
pub async fn run_diff(config: DiffConfig) -> Result<DiffReport> {
    let mut diff = DataDiff::new(config);
    diff.run().await
}

//! diff_data (data-diff): the data of two PostgreSQL databases compared - the SQL (INSERT, UPDATE, DELETE)
//! that makes a target's rows a reference's ([DataDiff]), and N copies of a table across a cluster ([cluster]).
//!
//! The data only. The schemas are sge's (Schema Guard Enterprise, `sge::dbm_compare`): a comprehensive diff,
//! schema and data, is sge's, and calls this crate for the data. Any project (sge, ytctl) uses diff_data as a
//! library, without its command line: `default-features = false`.
//!
//! Its messages are in six languages ([i18n]); an error starts with its code, `DA-Ennn`.
//! No unsafe code - the compiler holds it to that.
#![forbid(unsafe_code)]

// [i18n] locales/*.yml, compiled in; a language without a message is English
rust_i18n::i18n!("locales", fallback = "en");

#[macro_use]
pub mod i18n;
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
    ChunkStrategy, DiffReport, DiffStats, TableAnalysis, TableDiffReport, TableDiffStatus,
};

use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::NoTls;

/// Convenience function to create a bb8 connection pool from a PostgreSQL connection string.
pub async fn create_pool(connection_string: &str) -> Result<PgPool> {
    let manager = PostgresConnectionManager::new_from_stringlike(connection_string, NoTls)
        .map_err(|e| DataDiffError::Config(tr!("error.connection_string", err = e)))?;

    let pool = bb8::Pool::builder()
        .max_size(4)
        .build(manager)
        .await
        .map_err(|e| DataDiffError::Config(tr!("error.pool_create", err = e)))?;

    Ok(pool)
}

/// Convenience function to run a data diff with a config.
pub async fn run_diff(config: DiffConfig) -> Result<DiffReport> {
    let mut diff = DataDiff::new(config);
    diff.run().await
}

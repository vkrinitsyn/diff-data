use std::collections::{HashMap, HashSet};
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::NoTls;

pub type PgPool = Pool<PostgresConnectionManager<NoTls>>;

/// A pair of tables to compare.
#[derive(Debug, Clone)]
pub struct TablePair {
    /// Reference table name (schema.table or just table).
    pub reference: String,
    /// Target table name (schema.table or just table).
    pub target: String,
}

/// Progress event emitted during diff processing.
#[derive(Debug, Clone)]
pub enum ProgressEvent {
    /// Starting comparison for a table pair.
    TableStart {
        table: String,
        table_index: usize,
        total_tables: usize,
    },
    /// Row processed during INSERT/UPDATE phase.
    RowProcessed { table: String, row_number: u64 },
    /// INSERT/UPDATE phase completed for a table.
    TableDiffComplete {
        table: String,
        inserts: u64,
        updates: u64,
    },
    /// DELETE phase completed for a table.
    TableDeleteComplete { table: String, deletes: u64 },
    /// Sorting tables by foreign key dependencies.
    SortingDependencies,
    /// Starting ETC estimation phase.
    EstimationStart { total_tables: usize },
    /// ETC estimation complete for a single table.
    EstimationTableDone {
        table: String,
        estimated_seconds: f64,
        strategy: String,
    },
    /// ETC estimation complete for all tables.
    EstimationComplete { total_estimated_seconds: f64 },
    /// Progress within a table's MD5 timestamp chunk comparison.
    ChunkProgress {
        table: String,
        chunk_index: usize,
        total_chunks: usize,
    },
}

/// Configuration for a data diff operation: the data only - a schema compare is sge's.
#[derive(Clone)]
pub struct DiffConfig {
    /// Connection pool for the reference (source) database.
    pub reference_pool: PgPool,
    /// Connection pool for the target database.
    pub target_pool: PgPool,
    /// Explicit table pairs to compare. If empty, every table of `reference_schema` is compared.
    pub tables: Vec<TablePair>,
    /// Directory where output files are written.
    pub output_dir: PathBuf,
    /// Path for the master SQL script.
    pub output_file: PathBuf,
    /// Whether to generate DELETE statements for rows missing in reference.
    /// Default: false (matches original doc).
    pub include_delete: bool,
    /// Number of rows to fetch per chunk for comparison. Default: 25.
    pub chunk_size: usize,
    /// Column names to ignore during comparison (case-insensitive).
    pub columns_to_ignore: HashSet<String>,
    /// Alternate PK columns per table: table_name -> list of column names.
    pub alternate_keys: HashMap<String, Vec<String>>,
    /// Exclude the real PK columns from INSERT when alternate keys are used.
    pub exclude_real_pk: bool,
    /// Exclude ignored columns from generated SQL output.
    pub exclude_ignored_columns: bool,
    /// A condition on every reference table's rows, without the WHERE keyword (one there is taken off):
    /// only the rows it takes are compared. A delete is still of a target row the whole reference
    /// table lacks. Checksums are of every row.
    pub table_where: Option<String>,
    /// Write all SQL into `output_file` instead of separate per-table files and a master script:
    /// every table's INSERTs and UPDATEs, then the DELETEs.
    pub single_file: bool,
    /// Reference schema for auto-discovering tables. When set and `tables` is empty,
    /// all tables from this schema are compared.
    pub reference_schema: Option<String>,
    /// Target schema to compare against. Used with `reference_schema`.
    pub target_schema: Option<String>,
    /// Tables (lowercase names, schema left out) of over 1M rows without a timestamp column that are
    /// compared row by row (`IdChunkLargeExcluded`) rather than refused. Every table is compared.
    pub exclude_tables: HashSet<String>,
    /// Sort generated scripts by foreign key dependencies.
    /// INSERT/UPDATE scripts sorted for insert order, DELETE scripts for delete order.
    /// Default: true.
    pub check_dependencies: bool,
    /// Controls whether identity/auto-increment columns are included in generated SQL.
    /// `None` = use default behavior (exclude identity from INSERT),
    /// `Some(true)` = always include, `Some(false)` = always exclude.
    pub include_identity_columns: Option<bool>,
    /// Override auto-detect timestamp column name for chunking large tables.
    pub timestamp_column: Option<String>,
    /// Force run even if ETC exceeds `max_etc_seconds`.
    pub force: bool,
    /// Emit progress events (default: true).
    pub show_progress: bool,
    /// Reference database name, recorded in its checksum manifest.
    pub reference_db_name: Option<String>,
    /// Target database name, recorded in its checksum manifest.
    pub target_db_name: Option<String>,
    /// Maximum estimated time in seconds before aborting (default: 10800.0 = 3h).
    pub max_etc_seconds: f64,
    /// Optional output stream. When set, all SQL is written to this stream
    /// instead of per-table files. The stream receives the same content that
    /// would otherwise go into the master script and individual table files.
    pub output_stream: Option<Arc<Mutex<dyn io::Write + Send>>>,
    /// Optional progress callback.
    pub on_progress: Option<Arc<dyn Fn(ProgressEvent) + Send + Sync>>,
    /// Path to load pre-computed reference checksums (skip live ref DB query).
    pub reference_checksum_file: Option<PathBuf>,
    /// Path to load pre-computed target checksums (skip live target DB query).
    pub target_checksum_file: Option<PathBuf>,
    /// Path to save computed reference checksums for later reuse.
    pub save_reference_checksums: Option<PathBuf>,
    /// Path to save computed target checksums for later reuse.
    pub save_target_checksums: Option<PathBuf>,
    /// When true, only compute/compare checksums without generating SQL.
    pub checksum_only: bool,
}

impl std::fmt::Debug for DiffConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiffConfig")
            .field("tables", &self.tables)
            .field("output_dir", &self.output_dir)
            .field("output_file", &self.output_file)
            .field("include_delete", &self.include_delete)
            .field("chunk_size", &self.chunk_size)
            .field("columns_to_ignore", &self.columns_to_ignore)
            .field("alternate_keys", &self.alternate_keys)
            .field("exclude_real_pk", &self.exclude_real_pk)
            .field("exclude_ignored_columns", &self.exclude_ignored_columns)
            .field("table_where", &self.table_where)
            .field("single_file", &self.single_file)
            .field("reference_schema", &self.reference_schema)
            .field("target_schema", &self.target_schema)
            .field("exclude_tables", &self.exclude_tables)
            .field("check_dependencies", &self.check_dependencies)
            .field("include_identity_columns", &self.include_identity_columns)
            .field("timestamp_column", &self.timestamp_column)
            .field("force", &self.force)
            .field("show_progress", &self.show_progress)
            .field("reference_db_name", &self.reference_db_name)
            .field("target_db_name", &self.target_db_name)
            .field("max_etc_seconds", &self.max_etc_seconds)
            .field("output_stream", &self.output_stream.as_ref().map(|_| "..."))
            .field("on_progress", &self.on_progress.as_ref().map(|_| "..."))
            .field("reference_checksum_file", &self.reference_checksum_file)
            .field("target_checksum_file", &self.target_checksum_file)
            .field("save_reference_checksums", &self.save_reference_checksums)
            .field("save_target_checksums", &self.save_target_checksums)
            .field("checksum_only", &self.checksum_only)
            .finish()
    }
}

impl DiffConfig {
    pub fn new(reference_pool: PgPool, target_pool: PgPool, output_file: PathBuf) -> Self {
        let output_dir = output_file
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));

        Self {
            reference_pool,
            target_pool,
            tables: Vec::new(),
            output_dir,
            output_file,
            include_delete: false,
            chunk_size: 25,
            columns_to_ignore: HashSet::new(),
            alternate_keys: HashMap::new(),
            exclude_real_pk: false,
            exclude_ignored_columns: false,
            table_where: None,
            single_file: false,
            reference_schema: None,
            target_schema: None,
            exclude_tables: HashSet::new(),
            check_dependencies: true,
            include_identity_columns: None,
            timestamp_column: None,
            force: false,
            show_progress: true,
            reference_db_name: None,
            target_db_name: None,
            max_etc_seconds: 10800.0,
            output_stream: None,
            on_progress: None,
            reference_checksum_file: None,
            target_checksum_file: None,
            save_reference_checksums: None,
            save_target_checksums: None,
            checksum_only: false,
        }
    }

    /// Emit a progress event if a callback is configured.
    pub(crate) fn emit_progress(&self, event: ProgressEvent) {
        if let Some(ref cb) = self.on_progress {
            cb(event);
        }
    }
}

/// Parse a possibly schema-qualified table name into (schema, table).
/// Returns (None, name) if no schema is specified.
pub fn parse_table_name(name: &str) -> (Option<&str>, &str) {
    if let Some(dot_pos) = name.find('.') {
        let schema = &name[..dot_pos];
        let table = &name[dot_pos + 1..];
        (Some(schema), table)
    } else {
        (None, name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_table_name_splits_at_its_first_dot() {
        assert_eq!(parse_table_name("users"), (None, "users"));
        assert_eq!(parse_table_name("public.users"), (Some("public"), "users"));
        assert_eq!(parse_table_name("a.b.c"), (Some("a"), "b.c"));
    }

    #[tokio::test]
    async fn a_new_config_has_the_documented_defaults_and_reports_progress() {
        // the pools connect lazily: nothing is reached here
        let pool = crate::create_pool("host=127.0.0.1 port=1 user=nobody").await.unwrap();
        let cfg = DiffConfig::new(pool.clone(), pool.clone(), PathBuf::from("out/master.sql"));
        assert_eq!(cfg.output_dir, PathBuf::from("out"));
        assert_eq!(cfg.chunk_size, 25);
        assert!(!cfg.include_delete && cfg.check_dependencies && cfg.show_progress && !cfg.checksum_only);
        assert_eq!(cfg.max_etc_seconds, 10800.0);
        // no callback: nothing happens
        cfg.emit_progress(ProgressEvent::SortingDependencies);

        let root = DiffConfig::new(pool.clone(), pool.clone(), PathBuf::from("/"));
        assert_eq!(root.output_dir, PathBuf::from("."));

        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        let mut cfg = cfg;
        cfg.on_progress = Some(Arc::new(move |e: ProgressEvent| sink.lock().unwrap().push(format!("{:?}", e))));
        cfg.output_stream = Some(Arc::new(Mutex::new(Vec::<u8>::new())));
        cfg.emit_progress(ProgressEvent::SortingDependencies);
        assert_eq!(*seen.lock().unwrap(), vec!["SortingDependencies".to_string()]);

        let dbg = format!("{:?}", cfg);
        assert!(dbg.starts_with("DiffConfig {"));
        assert!(dbg.contains("output_stream: Some(\"...\")") && dbg.contains("on_progress: Some(\"...\")"));
        assert!(dbg.contains("max_etc_seconds: 10800.0"));
    }
}

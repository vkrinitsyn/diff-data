use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use tokio_postgres::Client;
use tracing::{debug, info, warn};

use crate::checksum::{self, compare_checksums, load_checksums, save_checksums};
use crate::config::{DiffConfig, ProgressEvent, TablePair, parse_table_name};
use crate::delete_sync::TableDeleteSync;
use crate::error::{DataDiffError, Result};
use crate::estimation::analyze_and_estimate;
use crate::metadata::{FkDependency, get_columns, get_fk_dependencies, list_tables, resolve_pk_columns, sort_for_insert};
use crate::table_diff::TableDataDiff;
use crate::types::{ColumnInfo, DiffReport, TableAnalysis, TableDiffReport};

/// Top-level orchestrator for comparing the data of two PostgreSQL databases - the data only: their schemas
/// are sge's to compare (a comprehensive diff, schema and data, is sge's).
///
/// 1. **Tables** - the configured pairs, else every table of `reference_schema`; with `check_dependencies`,
///    parents before children by their foreign keys
/// 2. **Analysis & ETC** - each table's chunk strategy and the estimated time to completion
/// 3. **Per-table diff** - INSERT/UPDATE SQL, and DELETE SQL with `include_delete`
/// 4. **Output** - a file per table and kind and the master script that includes them; or one stream
///    (`output_stream`, or `output_file` with `single_file`): the INSERTs and UPDATEs, then the DELETEs
pub struct DataDiff {
    config: DiffConfig,
    cancelled: bool,
}

/// A table's columns and its key on one side, the checksums' input
type ChecksumTable = (String, Vec<ColumnInfo>, Vec<ColumnInfo>);

impl DataDiff {
    pub fn new(config: DiffConfig) -> Self {
        Self { config, cancelled: false }
    }

    pub fn cancel(&mut self) {
        self.cancelled = true;
    }

    /// Run the full data diff operation.
    ///
    /// Routes to the appropriate method based on config:
    /// - `checksum_only` -> `run_compute_checksums()`: compute + save checksums, no SQL
    /// - both checksum files set -> `run_from_checksums()`: load checksums, compare, generate SQL for diffs only
    /// - otherwise -> default row-by-row comparison
    pub async fn run(&mut self) -> Result<DiffReport> {
        if self.config.checksum_only {
            return self.run_compute_checksums().await;
        }
        if self.config.reference_checksum_file.is_some() && self.config.target_checksum_file.is_some() {
            return self.run_from_checksums().await;
        }
        self.diff(None).await
    }

    /// Compute MD5 checksums of every row of every table, each side's, and save them to YAML: no SQL.
    /// A table without a key has no checksums on that side - a warning says so.
    ///
    /// Requires `save_reference_checksums` and/or `save_target_checksums` to be set.
    pub async fn run_compute_checksums(&mut self) -> Result<DiffReport> {
        if self.config.save_reference_checksums.is_none() && self.config.save_target_checksums.is_none() {
            return Err(DataDiffError::Config(tr!("error.checksum_only_no_file")));
        }

        let ref_conn = self.config.reference_pool.get().await?;
        let target_conn = self.config.target_pool.get().await?;
        let mut report = DiffReport::default();

        let tables = self.tables(&ref_conn).await?;
        let analyses = analyze_and_estimate(&ref_conn, &target_conn, &tables, &self.config).await?;

        if let Some(path) = self.config.save_reference_checksums.clone() {
            let info = self.checksum_tables(&ref_conn, &tables, true, &mut report.warnings).await?;
            let manifest = checksum::compute_db_checksums(
                &ref_conn,
                &info,
                &analyses,
                self.config.reference_db_name.as_deref(),
                self.config.reference_schema.as_deref(),
            )
            .await?;
            save_checksums(&manifest, &path)?;
            info!("Saved reference checksums to {}", path.display());
            for tc in &manifest.tables {
                let total_hashes: usize = tc.chunks.iter().map(|c| c.row_hashes.len()).sum();
                report.tables.push(TableDiffReport {
                    table_name: tc.table_name.clone(),
                    inserts: total_hashes as u64,
                    ..Default::default()
                });
            }
        }

        if let Some(path) = self.config.save_target_checksums.clone() {
            let info = self.checksum_tables(&target_conn, &tables, false, &mut report.warnings).await?;
            // each table's strategy is the reference's: the chunks of the two sides line up
            let target_analyses: Vec<TableAnalysis> = analyses
                .iter()
                .map(|a| TableAnalysis { ref_table: a.target_table.clone(), ..a.clone() })
                .collect();
            let manifest = checksum::compute_db_checksums(
                &target_conn,
                &info,
                &target_analyses,
                self.config.target_db_name.as_deref(),
                self.config.target_schema.as_deref(),
            )
            .await?;
            save_checksums(&manifest, &path)?;
            info!("Saved target checksums to {}", path.display());
        }

        Ok(report)
    }

    /// Generate SQL from pre-computed checksum files: for the tables whose checksums differ only, each
    /// compared and written as the default run writes it.
    ///
    /// Requires both `reference_checksum_file` and `target_checksum_file` to be set.
    pub async fn run_from_checksums(&mut self) -> Result<DiffReport> {
        let ref_checksum_path = self
            .config
            .reference_checksum_file
            .as_ref()
            .ok_or_else(|| DataDiffError::Config(tr!("error.no_reference_checksums")))?;
        let target_checksum_path = self
            .config
            .target_checksum_file
            .as_ref()
            .ok_or_else(|| DataDiffError::Config(tr!("error.no_target_checksums")))?;

        let ref_manifest = load_checksums(ref_checksum_path)?;
        let target_manifest = load_checksums(target_checksum_path)?;
        info!(
            "Loaded checksums: ref={} tables, target={} tables",
            ref_manifest.tables.len(),
            target_manifest.tables.len()
        );

        let diff_summaries = compare_checksums(&ref_manifest, &target_manifest);
        if diff_summaries.is_empty() {
            info!("All checksums match - no SQL to generate");
            return Ok(DiffReport::default());
        }
        for summary in &diff_summaries {
            info!(
                "Table '{}': {} differing chunks, {} missing in target, {} missing in ref, {} modified",
                summary.table_name,
                summary.differing_chunks.len(),
                summary.missing_in_target.len(),
                summary.missing_in_reference.len(),
                summary.modified.len(),
            );
        }
        let differing: HashSet<String> = diff_summaries.into_iter().map(|s| s.table_name).collect();
        self.diff(Some(&differing)).await
    }

    /// The diff of the tables - of those named in `only`, when given (by their names without schema)
    async fn diff(&mut self, only: Option<&HashSet<String>>) -> Result<DiffReport> {
        let ref_conn = self.config.reference_pool.get().await?;
        let target_conn = self.config.target_pool.get().await?;

        let mut tables = self.tables(&ref_conn).await?;
        if let Some(only) = only {
            tables.retain(|p| {
                let differs = only.contains(parse_table_name(&p.target).1) || only.contains(parse_table_name(&p.reference).1);
                if !differs {
                    debug!("Skipping table {} - checksums match", p.target);
                }
                differs
            });
        }

        let analyses = analyze_and_estimate(&ref_conn, &target_conn, &tables, &self.config).await?;
        let analysis_map: HashMap<String, TableAnalysis> = analyses.into_iter().map(|a| (a.ref_table.clone(), a)).collect();
        let tables = self.in_dependency_order(&ref_conn, tables).await?;

        let mut out = Output::open(&self.config)?;
        let mut report = DiffReport::default();
        for (table_idx, pair) in tables.iter().enumerate() {
            if self.cancelled {
                return Err(DataDiffError::Cancelled);
            }
            let analysis = analysis_map.get(&pair.reference).cloned();
            let table_report = self
                .diff_table(&ref_conn, &target_conn, pair, (table_idx, tables.len()), analysis, &mut out, &mut report.warnings)
                .await?;
            report.tables.push(table_report);
        }
        out.finish(&self.config, &mut report)?;
        Ok(report)
    }

    /// One table pair compared, its SQL to `out`. A table that cannot be compared (missing, no key, the
    /// target short of a reference column) is a warning, and the run goes on.
    #[allow(clippy::too_many_arguments)]
    async fn diff_table(
        &self,
        ref_conn: &Client,
        target_conn: &Client,
        pair: &TablePair,
        (table_index, total_tables): (usize, usize),
        analysis: Option<TableAnalysis>,
        out: &mut Output,
        warnings: &mut Vec<String>,
    ) -> Result<TableDiffReport> {
        let (_, ref_tname) = parse_table_name(&pair.reference);
        let (_, target_tname) = parse_table_name(&pair.target);
        self.config.emit_progress(ProgressEvent::TableStart {
            table: target_tname.to_string(),
            table_index,
            total_tables,
        });
        info!("Comparing table: {} -> {}", ref_tname, target_tname);

        let mut report = TableDiffReport { table_name: target_tname.to_string(), ..Default::default() };
        let alternate_keys = self.alternate_keys(pair);

        let mut diff = TableDataDiff::new(ref_conn, target_conn, pair.reference.clone(), pair.target.clone());
        diff.set_chunk_size(self.config.chunk_size);
        diff.set_columns_to_ignore(self.config.columns_to_ignore.clone());
        diff.set_alternate_keys(alternate_keys.clone());
        diff.set_exclude_real_pk(self.config.exclude_real_pk);
        diff.set_table_where(self.config.table_where.clone());
        diff.set_include_identity_columns(self.config.include_identity_columns);
        diff.set_on_progress(self.config.on_progress.clone());
        diff.set_analysis(analysis);
        if self.config.exclude_ignored_columns {
            diff.set_exclude_columns(self.config.columns_to_ignore.clone());
        }

        let mut sql = out.table(target_tname)?;
        match diff.execute(&mut sql.insert, &mut sql.update).await {
            Ok(stats) => {
                report.inserts = stats.inserts;
                report.updates = stats.updates;
                self.config.emit_progress(ProgressEvent::TableDiffComplete {
                    table: target_tname.to_string(),
                    inserts: stats.inserts,
                    updates: stats.updates,
                });
            }
            Err(e) if recoverable(&e) => {
                let warning = e.to_string();
                warn!("{}", warning);
                warnings.push(warning);
                sql.discard();
                return Ok(report);
            }
            Err(e) => return Err(e),
        }

        if self.config.include_delete {
            let mut del_sync = TableDeleteSync::new(target_conn, ref_conn, pair.target.clone(), pair.reference.clone());
            del_sync.set_chunk_size(self.config.chunk_size);
            del_sync.set_alternate_keys(alternate_keys);
            match del_sync.execute(&mut sql.delete).await {
                Ok(deletes) => {
                    report.deletes = deletes;
                    self.config.emit_progress(ProgressEvent::TableDeleteComplete {
                        table: target_tname.to_string(),
                        deletes,
                    });
                }
                Err(DataDiffError::NoPrimaryKey(_)) => debug!("Skipping delete sync for {} - no PK", pair.target),
                Err(e) => return Err(e),
            }
        }

        out.done(sql, &mut report)?;
        Ok(report)
    }

    /// The table pairs: the configured ones, else every table of `reference_schema`, each in
    /// `target_schema` (default the same)
    async fn tables(&self, ref_conn: &Client) -> Result<Vec<TablePair>> {
        let Some(ref_schema) = self.config.reference_schema.as_deref().filter(|_| self.config.tables.is_empty()) else {
            return Ok(self.config.tables.clone());
        };
        let target_schema = self.config.target_schema.as_deref().unwrap_or(ref_schema);
        Ok(list_tables(ref_conn, ref_schema)
            .await?
            .into_iter()
            .map(|t| TablePair { reference: format!("{}.{}", ref_schema, t), target: format!("{}.{}", target_schema, t) })
            .collect())
    }

    /// With `check_dependencies`, the pairs with each reference table after the tables it references
    /// (its foreign keys, within its schema), else as given; without, as given
    async fn in_dependency_order(&self, ref_conn: &Client, tables: Vec<TablePair>) -> Result<Vec<TablePair>> {
        if !self.config.check_dependencies || tables.len() < 2 {
            return Ok(tables);
        }
        self.config.emit_progress(ProgressEvent::SortingDependencies);
        let current: String = ref_conn.query_one("SELECT current_schema()", &[]).await?.get(0);
        let names: Vec<String> = tables
            .iter()
            .map(|p| {
                let (schema, table) = parse_table_name(&p.reference);
                format!("{}.{}", schema.unwrap_or(&current), table)
            })
            .collect();
        let mut schemas: Vec<&str> = names.iter().map(|n| parse_table_name(n).0.unwrap_or_default()).collect();
        schemas.sort_unstable();
        schemas.dedup();
        let mut deps = Vec::new();
        for schema in schemas {
            for d in get_fk_dependencies(ref_conn, schema).await? {
                deps.push(FkDependency {
                    table: format!("{}.{}", schema, d.table),
                    references_table: format!("{}.{}", schema, d.references_table),
                });
            }
        }
        let order = sort_for_insert(&names, &deps);
        let rank = |n: &String| order.iter().position(|o| o.eq_ignore_ascii_case(n)).unwrap_or(order.len());
        let mut ranked: Vec<(usize, TablePair)> = names.iter().map(rank).zip(tables).collect();
        ranked.sort_by_key(|(r, _)| *r);
        Ok(ranked.into_iter().map(|(_, p)| p).collect())
    }

    /// A pair's alternate key: by the target's name, schema-qualified or not, else the reference's
    fn alternate_keys(&self, pair: &TablePair) -> Option<Vec<String>> {
        [pair.target.as_str(), parse_table_name(&pair.target).1, pair.reference.as_str(), parse_table_name(&pair.reference).1]
            .iter()
            .find_map(|n| self.config.alternate_keys.get(*n))
            .cloned()
    }

    /// Each table of one side with its columns and its key; one without a key is left out, with a warning
    async fn checksum_tables(&self, conn: &Client, tables: &[TablePair], reference: bool, warnings: &mut Vec<String>)
        -> Result<Vec<ChecksumTable>>
    {
        let mut out = Vec::new();
        for pair in tables {
            let name = if reference { &pair.reference } else { &pair.target };
            let columns = get_columns(conn, name).await?;
            let keys = self.alternate_keys(pair);
            let (pk_cols, _) = resolve_pk_columns(&columns, keys.as_deref());
            if pk_cols.is_empty() {
                warnings.push(if reference {
                    tr!("error.no_key_reference_checksums", table = name)
                } else {
                    tr!("error.no_key_target_checksums", table = name)
                });
                continue;
            }
            out.push((name.clone(), columns, pk_cols));
        }
        Ok(out)
    }
}

/// A table that cannot be compared, which a run warns of and goes on past
fn recoverable(e: &DataDiffError) -> bool {
    matches!(e, DataDiffError::TableNotFound(_) | DataDiffError::NoPrimaryKey(_) | DataDiffError::ColumnMismatch { .. })
}

/// The output stream, locked
fn lock<'a>(out: &'a Mutex<dyn Write + Send + 'static>) -> Result<MutexGuard<'a, dyn Write + Send + 'static>> {
    out.lock().map_err(|e| DataDiffError::Config(tr!("error.stream_lock", err = e)))
}

/// Where a run's SQL goes
enum Output {
    /// a file per table and kind in `output_dir`, then the master script, `output_file`, that includes them
    Files { dir: PathBuf },
    /// one stream: each table's INSERTs and UPDATEs as it is compared, then every DELETE, children first
    /// with `check_dependencies`; `file` the one file (`single_file`) it is, if one
    Stream { out: Arc<Mutex<dyn Write + Send>>, deletes: Vec<Vec<u8>>, file: Option<PathBuf> },
}

/// One table's SQL by kind: its files, or buffers for the stream
struct TableSql {
    insert: Part,
    update: Part,
    delete: Part,
}

/// One kind of one table's SQL
enum Part {
    File { name: String, path: PathBuf, w: BufWriter<File> },
    Buf(Vec<u8>),
}

impl Write for Part {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        match self {
            Part::File { w, .. } => w.write(b),
            Part::Buf(v) => v.write(b),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Part::File { w, .. } => w.flush(),
            Part::Buf(_) => Ok(()),
        }
    }
}

impl Part {
    fn file(dir: &std::path::Path, name: String) -> io::Result<Part> {
        let path = dir.join(&name);
        Ok(Part::File { w: BufWriter::new(File::create(&path)?), name, path })
    }

    /// A file written: its name when it holds SQL, else removed
    fn keep(self) -> io::Result<Option<String>> {
        match self {
            Part::File { name, path, w } => {
                w.into_inner().map_err(|e| e.into_error())?;
                if fs::metadata(&path)?.len() > 0 {
                    Ok(Some(name))
                } else {
                    fs::remove_file(&path)?;
                    Ok(None)
                }
            }
            Part::Buf(_) => Ok(None),
        }
    }

    fn into_buf(self) -> Vec<u8> {
        match self {
            Part::Buf(v) => v,
            Part::File { .. } => Vec::new(),
        }
    }
}

impl TableSql {
    /// A table not compared: its files removed
    fn discard(self) {
        for part in [self.insert, self.update, self.delete] {
            if let Part::File { path, w, .. } = part {
                drop(w);
                let _ = fs::remove_file(path);
            }
        }
    }
}

impl Output {
    fn open(config: &DiffConfig) -> Result<Output> {
        if let Some(out) = &config.output_stream {
            return Ok(Output::Stream { out: out.clone(), deletes: Vec::new(), file: None });
        }
        if config.single_file {
            if let Some(dir) = config.output_file.parent().filter(|d| !d.as_os_str().is_empty()) {
                fs::create_dir_all(dir)?;
            }
            let file: Arc<Mutex<dyn Write + Send>> = Arc::new(Mutex::new(BufWriter::new(File::create(&config.output_file)?)));
            return Ok(Output::Stream { out: file, deletes: Vec::new(), file: Some(config.output_file.clone()) });
        }
        fs::create_dir_all(&config.output_dir)?;
        Ok(Output::Files { dir: config.output_dir.clone() })
    }

    /// Where one table's SQL is written
    fn table(&self, table: &str) -> Result<TableSql> {
        Ok(match self {
            Output::Files { dir } => TableSql {
                insert: Part::file(dir, format!("{}_$insert.sql", table))?,
                update: Part::file(dir, format!("{}_$update.sql", table))?,
                delete: Part::file(dir, format!("{}_$delete.sql", table))?,
            },
            Output::Stream { .. } => {
                TableSql { insert: Part::Buf(Vec::new()), update: Part::Buf(Vec::new()), delete: Part::Buf(Vec::new()) }
            }
        })
    }

    /// A table compared: its files kept, or its INSERTs and UPDATEs streamed and its DELETEs kept for the end
    fn done(&mut self, sql: TableSql, report: &mut TableDiffReport) -> Result<()> {
        match self {
            Output::Files { .. } => {
                report.insert_file = sql.insert.keep()?;
                report.update_file = sql.update.keep()?;
                report.delete_file = sql.delete.keep()?;
            }
            Output::Stream { out, deletes, .. } => {
                let mut locked = lock(out)?;
                locked.write_all(&sql.insert.into_buf())?;
                locked.write_all(&sql.update.into_buf())?;
                let delete = sql.delete.into_buf();
                if !delete.is_empty() {
                    deletes.push(delete);
                }
            }
        }
        Ok(())
    }

    /// Every table compared: the master script written, or the DELETEs streamed and the stream flushed
    fn finish(self, config: &DiffConfig, report: &mut DiffReport) -> Result<()> {
        match self {
            Output::Files { .. } => {
                write_master_script(config, report)?;
                report.master_script = Some(config.output_file.to_string_lossy().into_owned());
            }
            Output::Stream { out, mut deletes, file } => {
                if config.check_dependencies {
                    deletes.reverse();
                }
                let mut locked = lock(&out)?;
                for d in &deletes {
                    locked.write_all(d)?;
                }
                locked.flush()?;
                report.master_script = file.map(|f| f.to_string_lossy().into_owned());
            }
        }
        Ok(())
    }
}

/// The master SQL script that includes every table's scripts: INSERTs and UPDATEs in table order, then the
/// DELETEs - children first with `check_dependencies`.
fn write_master_script(config: &DiffConfig, report: &DiffReport) -> Result<()> {
    let mut out = BufWriter::new(File::create(&config.output_file)?);

    writeln!(out, "-- ----------------------------------------------------------------")?;
    writeln!(out, "-- Data migration script")?;
    writeln!(out, "-- ----------------------------------------------------------------")?;
    writeln!(out)?;
    writeln!(out, "-- Tables included:")?;
    for table in &report.tables {
        writeln!(out, "-- {}", table.table_name)?;
    }
    writeln!(out)?;

    if !report.warnings.is_empty() {
        writeln!(out, "-- Warnings:")?;
        for w in &report.warnings {
            writeln!(out, "-- {}", w)?;
        }
        writeln!(out)?;
    }

    writeln!(out, "-- ----------------------")?;
    writeln!(out, "-- UPDATE/INSERT scripts")?;
    writeln!(out, "-- ----------------------")?;

    let mut has_updates = false;
    for table in &report.tables {
        if let Some(ref f) = table.insert_file {
            writeln!(out, "\\i '{}'", f)?;
            has_updates = true;
        } else {
            writeln!(out, "-- No INSERTs for {} necessary", table.table_name)?;
        }
        if let Some(ref f) = table.update_file {
            writeln!(out, "\\i '{}'", f)?;
            has_updates = true;
        } else {
            writeln!(out, "-- No UPDATEs for {} necessary", table.table_name)?;
        }
    }

    if has_updates {
        writeln!(out)?;
        writeln!(out, "COMMIT;")?;
    }

    if config.include_delete {
        writeln!(out)?;
        writeln!(out, "-- ---------------")?;
        writeln!(out, "-- DELETE scripts")?;
        writeln!(out, "-- ---------------")?;

        let mut tables: Vec<&TableDiffReport> = report.tables.iter().collect();
        if config.check_dependencies {
            tables.reverse();
        }
        for table in &tables {
            if let Some(ref f) = table.delete_file {
                writeln!(out, "\\i '{}'", f)?;
            } else {
                writeln!(out, "-- No DELETEs for {} necessary", table.table_name)?;
            }
        }

        if tables.iter().any(|t| t.delete_file.is_some()) {
            writeln!(out)?;
            writeln!(out, "COMMIT;")?;
        }
    }

    out.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("diff_data_unit_{}_{}", name, std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    async fn config(out: &Path) -> DiffConfig {
        // the pools connect lazily: nothing is reached here
        let pool = crate::create_pool("host=127.0.0.1 port=1 user=nobody").await.unwrap();
        DiffConfig::new(pool.clone(), pool, out.join("master.sql"))
    }

    #[test]
    fn a_table_that_cannot_be_compared_is_recoverable_and_nothing_else_is() {
        assert!(recoverable(&DataDiffError::TableNotFound("t".into())));
        assert!(recoverable(&DataDiffError::NoPrimaryKey("t".into())));
        assert!(recoverable(&DataDiffError::ColumnMismatch { ref_table: "a".into(), target_table: "b".into() }));
        assert!(!recoverable(&DataDiffError::Cancelled));
    }

    #[test]
    fn an_empty_file_is_removed_a_full_one_kept_and_a_discarded_table_leaves_none() {
        let dir = scratch("parts");
        let mut full = Part::file(&dir, "full.sql".into()).unwrap();
        full.write_all(b"x").unwrap();
        full.flush().unwrap();
        assert_eq!(full.keep().unwrap(), Some("full.sql".to_string()));
        assert_eq!(Part::file(&dir, "empty.sql".into()).unwrap().keep().unwrap(), None);
        assert!(dir.join("full.sql").exists() && !dir.join("empty.sql").exists());
        assert_eq!(Part::Buf(b"y".to_vec()).keep().unwrap(), None);
        assert!(Part::file(&dir, "f.sql".into()).unwrap().into_buf().is_empty());

        let files = Output::Files { dir: dir.clone() };
        let sql = files.table("t").unwrap();
        assert!(dir.join("t_$insert.sql").exists());
        sql.discard();
        assert!(!dir.join("t_$insert.sql").exists() && !dir.join("t_$delete.sql").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_stream_takes_each_tables_inserts_and_updates_then_the_deletes_children_first() {
        let dir = scratch("stream");
        let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
        let mut config = config(&dir).await;
        config.output_stream = Some(buf.clone());
        let mut out = Output::open(&config).unwrap();
        for (table, sql) in [("parent", "p"), ("child", "c")] {
            let mut t = out.table(table).unwrap();
            write!(t.insert, "i{} ", sql).unwrap();
            write!(t.update, "u{} ", sql).unwrap();
            write!(t.delete, "d{} ", sql).unwrap();
            assert!(t.insert.flush().is_ok());
            out.done(t, &mut TableDiffReport::default()).unwrap();
        }
        let empty = out.table("none").unwrap();
        out.done(empty, &mut TableDiffReport::default()).unwrap();
        let mut report = DiffReport::default();
        out.finish(&config, &mut report).unwrap();
        assert_eq!(String::from_utf8(buf.lock().unwrap().clone()).unwrap(), "ip up ic uc dc dp ");
        assert!(report.master_script.is_none());

        // the one file: as the stream, its path the report's script
        config.output_stream = None;
        config.single_file = true;
        config.check_dependencies = false;
        config.output_file = dir.join("sub").join("all.sql");
        let mut out = Output::open(&config).unwrap();
        for table in ["a", "b"] {
            let mut t = out.table(table).unwrap();
            write!(t.delete, "d{} ", table).unwrap();
            out.done(t, &mut TableDiffReport::default()).unwrap();
        }
        let mut report = DiffReport::default();
        out.finish(&config, &mut report).unwrap();
        assert_eq!(fs::read_to_string(dir.join("sub").join("all.sql")).unwrap(), "da db ", "as given, unordered");
        assert_eq!(report.master_script.as_deref(), Some(dir.join("sub").join("all.sql").to_str().unwrap()));

        // a poisoned stream is refused
        let poisoned: Arc<Mutex<dyn Write + Send>> = Arc::new(Mutex::new(Vec::<u8>::new()));
        let p = poisoned.clone();
        let _ = std::thread::spawn(move || {
            let _g = p.lock().unwrap();
            panic!("poison");
        })
        .join();
        assert!(matches!(lock(&poisoned), Err(DataDiffError::Config(m)) if m.starts_with("DA-E014 ")));
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn the_master_script_includes_each_file_and_says_what_has_none() {
        let dir = scratch("master");
        let mut config = config(&dir).await;
        config.include_delete = true;
        let report = DiffReport {
            tables: vec![
                TableDiffReport {
                    table_name: "users".into(),
                    inserts: 1,
                    updates: 1,
                    deletes: 1,
                    insert_file: Some("users_$insert.sql".into()),
                    update_file: Some("users_$update.sql".into()),
                    delete_file: Some("users_$delete.sql".into()),
                },
                TableDiffReport { table_name: "orders".into(), ..Default::default() },
            ],
            warnings: vec!["DA-E005 table not found: gone".into()],
            master_script: None,
        };
        write_master_script(&config, &report).unwrap();
        let text = fs::read_to_string(dir.join("master.sql")).unwrap();
        assert!(text.contains("-- Tables included:\n-- users\n-- orders\n"));
        assert!(text.contains("-- Warnings:\n-- DA-E005 table not found: gone\n"));
        assert!(text.contains("\\i 'users_$insert.sql'\n\\i 'users_$update.sql'\n"));
        assert!(text.contains("-- No INSERTs for orders necessary\n-- No UPDATEs for orders necessary\n"));
        assert!(text.contains("-- DELETE scripts"));
        // children first: in delete order, the reverse of the tables'
        assert!(text.contains("-- No DELETEs for orders necessary\n\\i 'users_$delete.sql'\n"), "{}", text);
        assert_eq!(text.matches("COMMIT;").count(), 2);

        // unordered: the deletes as the tables are
        config.check_dependencies = false;
        write_master_script(&config, &report).unwrap();
        let text = fs::read_to_string(dir.join("master.sql")).unwrap();
        assert!(text.contains("\\i 'users_$delete.sql'\n-- No DELETEs for orders necessary\n"), "{}", text);

        // nothing to include, no deletes asked: no COMMIT, no DELETE section
        config.include_delete = false;
        let quiet = DiffReport { tables: vec![TableDiffReport { table_name: "orders".into(), ..Default::default() }], ..Default::default() };
        write_master_script(&config, &quiet).unwrap();
        let text = fs::read_to_string(dir.join("master.sql")).unwrap();
        assert!(!text.contains("COMMIT;") && !text.contains("DELETE scripts") && !text.contains("Warnings"));
        let _ = fs::remove_dir_all(&dir);
    }
}

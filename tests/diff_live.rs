//! The reference/target diff against a live PostgreSQL: two databases on one server per test, the reference
//! and the target, compared by the crate's whole flow (`DataDiff`) and by its parts.
//! Run with `DIFF_DATA_PG="host=127.0.0.1 port=5432 user=postgres" cargo test --test diff_live`; without it
//! every test does nothing. Each test creates and drops its own `dd_<test>_ref` and `dd_<test>_tgt`, as a
//! superuser: a "large" table is one whose `pg_class.reltuples` the test sets past 1M rows.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use diff_data::chunk_strategy::{determine_strategy, get_row_count, get_timestamp_range};
use diff_data::checksum::{compute_db_checksums, compute_table_checksums};
use diff_data::config::{ProgressEvent, TablePair};
use diff_data::delete_sync::TableDeleteSync;
use diff_data::estimation::{analyze_and_estimate, estimate_table_time};
use diff_data::metadata::{get_columns, get_fk_dependencies, list_tables, resolve_pk_columns, table_exists};
use diff_data::table_diff::TableDataDiff;
use diff_data::types::{extract_row, extract_row_value, ChunkStrategy, ColumnInfo, RowValue, TableAnalysis};
use diff_data::{
    ChecksumManifest, DataDiff, DataDiffError, DiffConfig, PgPool, create_pool,
    load_checksums, run_diff, save_checksums,
};
use postgres_types::Type;
use tokio_postgres::{Client, NoTls};

// ── the fixture ─────────────────────────────────────────────────────────────────────────────────────

/// The reference: every table the flow meets - one that differs (users), an identity key (orders), one the
/// same on both sides (same), no primary key (nopk), one the target lacks (only_ref), quoted names (Mixed),
/// a key the target lacks (tpk) and a large table chunked by its timestamp (events, 11 days)
const REF_SQL: &str = r#"
    CREATE TABLE users (id int PRIMARY KEY, name text, email varchar(100), active bool, score float8, updated_at timestamp);
    CREATE TABLE orders (id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY, user_id int REFERENCES users (id), total int, note text);
    CREATE TABLE same (id int PRIMARY KEY, v text);
    CREATE TABLE nopk (a int, b text);
    CREATE TABLE only_ref (id int PRIMARY KEY);
    CREATE TABLE "Mixed" (id int PRIMARY KEY, "Val" text);
    CREATE TABLE tpk (id int PRIMARY KEY, v text);
    CREATE TABLE events (id text PRIMARY KEY, payload text, updated_at timestamp NOT NULL);
    INSERT INTO users VALUES
        (1, 'a', 'a@x', true, 1.0, '2024-01-01'), (2, 'b', 'b@x', true, 2.0, '2024-01-02'),
        (3, 'c', 'c@x', false, 3.0, '2024-01-03'), (4, 'd', NULL, NULL, 4.0, '2024-01-04'),
        (5, 'e', 'e@x', true, 5.0, '2024-01-05');
    INSERT INTO orders (user_id, total, note) VALUES (1, 10, 'n1'), (2, 20, 'n2');
    INSERT INTO same VALUES (1, 'v');
    INSERT INTO nopk VALUES (1, 'a');
    INSERT INTO only_ref VALUES (1);
    INSERT INTO "Mixed" VALUES (1, 'x');
    INSERT INTO tpk VALUES (1, 'a'), (2, 'b');
    INSERT INTO events SELECT 'e' || g, 'p' || g, timestamp '2024-01-01' + (g - 1) * interval '1 day'
        FROM generate_series(1, 11) g;
    UPDATE pg_class SET reltuples = 2000000 WHERE oid = 'public.events'::regclass;
"#;

/// The target: users has an extra column, a changed row (2), a missing one (3), a changed ignored column
/// (4) and an extra row (6); orders lacks a row; Mixed differs; tpk has no key and lacks a row; events has
/// a changed row (e2), a missing one (e3) and an extra one (e99); only_ref is not there
const TGT_SQL: &str = r#"
    CREATE TABLE users (id int PRIMARY KEY, name text, email varchar(100), active bool, score float8, updated_at timestamp, extra int);
    CREATE TABLE orders (id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY, user_id int REFERENCES users (id), total int, note text);
    CREATE TABLE same (id int PRIMARY KEY, v text);
    CREATE TABLE nopk (a int, b text);
    CREATE TABLE "Mixed" (id int PRIMARY KEY, "Val" text);
    CREATE TABLE tpk (id int, v text);
    CREATE TABLE events (id text PRIMARY KEY, payload text, updated_at timestamp NOT NULL);
    INSERT INTO users VALUES
        (1, 'a', 'a@x', true, 1.0, '2024-01-01', 7), (2, 'B', 'b@x', true, 2.0, '2024-01-02', NULL),
        (4, 'd', NULL, NULL, 9.0, '2024-01-04', NULL), (5, 'e', 'e@x', true, 5.0, '2024-01-05', NULL),
        (6, 'f', 'f@x', true, 6.0, '2024-01-06', NULL);
    INSERT INTO orders (user_id, total, note) VALUES (1, 10, 'n1');
    INSERT INTO same VALUES (1, 'v');
    INSERT INTO nopk VALUES (1, 'a');
    INSERT INTO "Mixed" VALUES (1, 'y');
    INSERT INTO tpk VALUES (1, 'a');
    INSERT INTO events SELECT 'e' || g, CASE WHEN g = 2 THEN 'changed' ELSE 'p' || g END,
        timestamp '2024-01-01' + (g - 1) * interval '1 day' FROM generate_series(1, 11) g WHERE g <> 3;
    INSERT INTO events VALUES ('e99', 'extra', '2024-01-05');
"#;

async fn connect(url: &str) -> Client {
    let (c, conn) = tokio_postgres::connect(url, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    c
}

/// A reference and a target database of one test
struct Pair {
    base: String,
    r: String,
    t: String,
}

impl Pair {
    /// Two fresh databases set up by these scripts; None without DIFF_DATA_PG
    async fn new(name: &str, ref_sql: &str, tgt_sql: &str) -> Option<Pair> {
        let base = std::env::var("DIFF_DATA_PG").ok()?;
        // the messages asserted are English ones: every test sets the same, the language is the process's
        diff_data::i18n::set_lang("en");
        let pair = Pair { r: format!("dd_{}_ref", name), t: format!("dd_{}_tgt", name), base };
        let admin = connect(&pair.url("postgres")).await;
        for db in [&pair.r, &pair.t] {
            admin.batch_execute(&format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", db)).await.unwrap();
            admin.batch_execute(&format!("CREATE DATABASE {}", db)).await.unwrap();
        }
        connect(&pair.url(&pair.r)).await.batch_execute(ref_sql).await.expect("the reference");
        connect(&pair.url(&pair.t)).await.batch_execute(tgt_sql).await.expect("the target");
        Some(pair)
    }

    fn url(&self, db: &str) -> String {
        format!("{} dbname={}", self.base, db)
    }

    async fn clients(&self) -> (Client, Client) {
        (connect(&self.url(&self.r)).await, connect(&self.url(&self.t)).await)
    }

    async fn pools(&self) -> (PgPool, PgPool) {
        (create_pool(&self.url(&self.r)).await.unwrap(), create_pool(&self.url(&self.t)).await.unwrap())
    }

    /// A config over these tables, its output in a fresh directory
    async fn config(&self, tables: &[&str], out: &Path) -> DiffConfig {
        let (rp, tp) = self.pools().await;
        let mut c = DiffConfig::new(rp, tp, out.join("master.sql"));
        c.tables = tables.iter().map(|t| TablePair { reference: t.to_string(), target: t.to_string() }).collect();
        c.include_delete = true;
        c.chunk_size = 2;
        c.columns_to_ignore = ["score".to_string()].into();
        c.exclude_ignored_columns = true;
        c.alternate_keys = HashMap::from([("Mixed".to_string(), vec!["id".to_string()])]);
        c
    }

    async fn drop(self) {
        let admin = connect(&self.url("postgres")).await;
        for db in [&self.r, &self.t] {
            admin.batch_execute(&format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", db)).await.unwrap();
        }
    }
}

fn out_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("diff_data_live_{}_{}", name, std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    dir
}

/// The events a progress callback kept
type Seen = Arc<Mutex<Vec<ProgressEvent>>>;

/// A progress callback that keeps every event
fn recorder() -> (Seen, Arc<dyn Fn(ProgressEvent) + Send + Sync>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    (seen, Arc::new(move |e| sink.lock().unwrap().push(e)))
}

fn table<'a>(report: &'a diff_data::DiffReport, name: &str) -> &'a diff_data::TableDiffReport {
    report.tables.iter().find(|t| t.table_name == name).unwrap_or_else(|| panic!("{} in {:?}", name, report))
}

fn counts(report: &diff_data::DiffReport, name: &str) -> (u64, u64, u64) {
    let t = table(report, name);
    (t.inserts, t.updates, t.deletes)
}

fn read(dir: &Path, file: &str) -> String {
    fs::read_to_string(dir.join(file)).unwrap_or_else(|e| panic!("{}: {}", file, e))
}

/// One table diff's counts and its INSERT and UPDATE SQL
async fn run_table(diff: &mut TableDataDiff<'_>) -> diff_data::Result<(u64, u64, String, String)> {
    let mut ins = Vec::new();
    let mut upd = Vec::new();
    let s = diff.execute(&mut ins, &mut upd).await?;
    Ok((s.inserts, s.updates, String::from_utf8(ins).unwrap(), String::from_utf8(upd).unwrap()))
}

/// The error a table diff of these tables stops with
async fn table_err(r: &Client, t: &Client, rt: &str, tt: &str, analysis: Option<TableAnalysis>) -> DataDiffError {
    let mut d = TableDataDiff::new(r, t, rt.to_string(), tt.to_string());
    d.set_analysis(analysis);
    d.execute(&mut Vec::new(), &mut Vec::new()).await.unwrap_err()
}

/// One delete sync's count and its SQL
async fn run_delete(sync: &mut TableDeleteSync<'_>) -> diff_data::Result<(u64, String)> {
    let mut out = Vec::new();
    let n = sync.execute(&mut out).await?;
    Ok((n, String::from_utf8(out).unwrap()))
}

/// A table's strategy as the reference decides it
async fn strategy_of(r: &Client, table: &str, ts: Option<&str>, excluded: bool) -> diff_data::Result<(ChunkStrategy, Option<String>)> {
    let cols = get_columns(r, table).await.unwrap();
    determine_strategy(r, table, &cols, ts, excluded).await
}

// ── the whole flow ──────────────────────────────────────────────────────────────────────────────────

/// File mode: one file per table and kind, a master script, and a warning for each table that cannot be
/// compared
#[tokio::test]
async fn a_diff_writes_a_file_per_table_and_a_master_script() {
    let Some(pair) = Pair::new("files", REF_SQL, TGT_SQL).await else { return };
    let out = out_dir("files");
    let tables = ["users", "orders", "same", "nopk", "only_ref", "Mixed", "tpk", "events"];
    let mut config = pair.config(&tables, &out).await;
    let (seen, cb) = recorder();
    config.on_progress = Some(cb);

    let report = DataDiff::new(config).run().await.expect("the diff");

    assert_eq!(counts(&report, "users"), (1, 1, 1), "3 missing, 2 changed, 6 extra; 4 differs only by score");
    assert_eq!(counts(&report, "orders"), (1, 0, 0));
    assert_eq!(counts(&report, "same"), (0, 0, 0));
    assert_eq!(counts(&report, "Mixed"), (0, 1, 0));
    assert_eq!(counts(&report, "tpk"), (1, 0, 0), "the target has no key: no deletes");
    assert_eq!(counts(&report, "events"), (1, 1, 1), "e3 missing, e2 changed, e99 extra");
    assert_eq!(counts(&report, "only_ref"), (0, 0, 0));
    assert_eq!(report.warnings, vec!["DA-E004 table nopk has no primary key", "DA-E005 table not found: only_ref"]);

    let users = read(&out, "users_$insert.sql");
    assert!(users.contains("-- Data diff for table: users"));
    assert!(users.contains("INSERT INTO users (id, name, email, active, updated_at) VALUES (3, 'c', 'c@x', FALSE, '2024-01-03 00:00:00'::timestamp);"), "{}", users);
    assert!(read(&out, "users_$update.sql").contains("UPDATE users SET name = 'b' WHERE id = 2;"));
    assert!(read(&out, "users_$delete.sql").contains("DELETE FROM users WHERE id = 6;"));
    assert!(read(&out, "orders_$insert.sql").contains("INSERT INTO orders (user_id, total, note) VALUES (2, 20, 'n2');"));
    assert!(read(&out, "Mixed_$update.sql").contains("UPDATE \"Mixed\" SET \"Val\" = 'x' WHERE id = 1;"));
    assert!(read(&out, "events_$insert.sql").contains("VALUES ('e3', 'p3', "));
    assert!(read(&out, "events_$update.sql").contains("SET payload = 'p2' WHERE id = 'e2';"));
    assert!(read(&out, "events_$delete.sql").contains("DELETE FROM events WHERE id = 'e99';"));
    for gone in ["same_$insert.sql", "same_$update.sql", "same_$delete.sql", "tpk_$delete.sql", "nopk_$insert.sql"] {
        assert!(!out.join(gone).exists(), "{} is empty and removed", gone);
    }

    let master = read(&out, "master.sql");
    assert_eq!(report.master_script.as_deref(), Some(out.join("master.sql").to_str().unwrap()));
    assert!(master.contains("\\i 'users_$insert.sql'") && master.contains("\\i 'events_$delete.sql'"));
    assert!(master.contains("-- No INSERTs for same necessary"));

    // children's deletes first: events, tpk, ..., users last of the tables before it
    let deletes = &master[master.find("-- DELETE scripts").unwrap()..];
    assert!(deletes.find("events_$delete.sql").unwrap() < deletes.find("users_$delete.sql").unwrap(), "{}", deletes);
    let seen = seen.lock().unwrap().clone();
    assert!(seen.iter().any(|e| matches!(e, ProgressEvent::SortingDependencies)));
    assert!(seen.iter().any(|e| matches!(e, ProgressEvent::EstimationTableDone { table, strategy, .. } if table == "events" && strategy.starts_with("Md5TimestampChunk"))));
    assert!(seen.iter().any(|e| matches!(e, ProgressEvent::ChunkProgress { total_chunks: 10, .. })));
    assert!(seen.iter().any(|e| matches!(e, ProgressEvent::TableDeleteComplete { table, deletes: 1 } if table == "users")));

    let _ = fs::remove_dir_all(&out);
    pair.drop().await;
}

/// Stream mode, no schema comparison: the same SQL in one stream, no file; a table the target lacks is a
/// warning
#[tokio::test]
async fn a_streamed_diff_writes_its_sql_to_the_stream() {
    let Some(pair) = Pair::new("stream", REF_SQL, TGT_SQL).await else { return };
    let out = out_dir("stream");
    let tables = ["users", "same", "nopk", "only_ref", "tpk", "public.events"];
    let mut config = pair.config(&tables, &out).await;
    let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
    config.output_stream = Some(buf.clone());
    let (seen, cb) = recorder();
    config.on_progress = Some(cb);

    let report = run_diff(config).await.expect("the diff");

    assert_eq!(counts(&report, "users"), (1, 1, 1));
    assert_eq!(counts(&report, "same"), (0, 0, 0));
    assert_eq!(counts(&report, "tpk"), (1, 0, 0));
    assert_eq!(counts(&report, "events"), (1, 1, 1));
    assert!(report.master_script.is_none() && !out.exists(), "nothing is written but the stream");
    assert!(report.warnings.contains(&"DA-E005 table not found: only_ref".to_string()), "{:?}", report.warnings);
    assert!(report.warnings.contains(&"DA-E004 table nopk has no primary key".to_string()));

    let sql = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(sql.contains("UPDATE users SET name = 'b' WHERE id = 2;"));
    assert!(sql.contains("DELETE FROM users WHERE id = 6;"));
    assert!(sql.contains("INSERT INTO tpk (id, v) VALUES (2, 'b');"));
    assert!(sql.contains("DELETE FROM public.events WHERE id = 'e99';"));
    assert!(!sql.contains("same"));
    // every DELETE after every INSERT and UPDATE
    assert!(sql.rfind("INSERT INTO").unwrap() < sql.find("DELETE FROM").unwrap(), "{}", sql);
    assert!(seen.lock().unwrap().iter().any(|e| matches!(e, ProgressEvent::TableDiffComplete { table, inserts: 1, updates: 1 } if table == "users")));

    pair.drop().await;
}

/// Checksums computed apart, saved, then the SQL generated from them for the tables that differ only -
/// to files and to a stream; matching checksums generate nothing
#[tokio::test]
async fn the_sql_is_generated_from_checksums_computed_apart() {
    let Some(pair) = Pair::new("checksums", REF_SQL, TGT_SQL).await else { return };
    let out = out_dir("checksums");
    fs::create_dir_all(&out).unwrap();
    let (ref_file, tgt_file) = (out.join("ref.yaml"), out.join("tgt.yaml"));
    let summed = ["users", "same", "Mixed", "orders", "events", "tpk", "nopk"];

    // checksum_only needs somewhere to save
    let mut config = pair.config(&summed, &out).await;
    config.checksum_only = true;
    assert!(matches!(DataDiff::new(config.clone()).run().await, Err(DataDiffError::Config(_))));

    config.save_reference_checksums = Some(ref_file.clone());
    config.save_target_checksums = Some(tgt_file.clone());
    config.reference_db_name = Some(pair.r.clone());
    config.reference_schema = Some("public".into());
    let report = DataDiff::new(config.clone()).run().await.expect("the checksums");
    assert_eq!(table(&report, "events").inserts, 11, "the reference's rows hashed");
    assert_eq!(table(&report, "users").inserts, 5, "one checksum a row, by an integer key too");
    // a table without a key has no checksums on that side, and says so; the run goes on
    assert!(report.tables.iter().all(|t| t.table_name != "nopk"));
    assert_eq!(report.warnings, vec![
        "DA-E021 table nopk has no primary key: it has no reference checksums",
        "DA-E022 table tpk has no primary key: it has no target checksums",
        "DA-E022 table nopk has no primary key: it has no target checksums",
    ]);
    let reference = load_checksums(&ref_file).unwrap();
    let target = load_checksums(&tgt_file).unwrap();
    assert_eq!(reference.db_name.as_deref(), Some(pair.r.as_str()));
    let events = reference.tables.iter().find(|t| t.table_name == "events").unwrap();
    assert_eq!((events.chunks.len(), events.timestamp_column.as_deref()), (10, Some("updated_at")));
    assert!(target.tables.iter().any(|t| t.table_name == "events"));
    assert!(reference.tables.iter().any(|t| t.table_name == "tpk") && !target.tables.iter().any(|t| t.table_name == "tpk"));
    let users = reference.tables.iter().find(|t| t.table_name == "users").unwrap();
    let mut keys: Vec<&String> = users.chunks[0].row_hashes.keys().collect();
    keys.sort();
    assert_eq!(keys, vec!["1", "2", "3", "4", "5"]);

    // tpk is on the reference's checksums only: it differs whole. A reference whose checksums were
    // computed elsewhere (by another key) also lists nopk
    let mut with_more: ChecksumManifest = reference.clone();
    let mut nopk = with_more.tables[0].clone();
    nopk.table_name = "nopk".to_string();
    with_more.tables.push(nopk);
    save_checksums(&with_more, &ref_file).unwrap();

    let all = ["users", "same", "Mixed", "orders", "events", "tpk", "nopk"];
    let mut config = pair.config(&all, &out).await;
    config.reference_checksum_file = Some(ref_file.clone());
    config.target_checksum_file = Some(tgt_file.clone());
    let (seen, cb) = recorder();
    config.on_progress = Some(cb);
    let report = DataDiff::new(config.clone()).run().await.expect("the SQL from checksums");
    assert!(report.tables.iter().all(|t| t.table_name != "same"), "a table whose checksums match is skipped");
    assert_eq!(counts(&report, "users"), (1, 1, 1));
    assert_eq!(counts(&report, "events"), (1, 1, 1));
    assert_eq!(counts(&report, "tpk"), (1, 0, 0));
    assert_eq!(report.warnings, vec!["DA-E004 table nopk has no primary key"]);
    assert!(read(&out, "users_$delete.sql").contains("DELETE FROM users WHERE id = 6;"));
    assert!(read(&out, "master.sql").contains("\\i 'events_$update.sql'"));
    assert!(!out.join("same_$insert.sql").exists());
    assert!(seen.lock().unwrap().iter().any(|e| matches!(e, ProgressEvent::TableDeleteComplete { .. })));

    // the same to a stream
    let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
    let mut streamed = config.clone();
    streamed.output_stream = Some(buf.clone());
    let report = DataDiff::new(streamed).run().await.expect("the streamed SQL from checksums");
    assert_eq!(counts(&report, "users"), (1, 1, 1));
    assert!(report.master_script.is_none());
    let sql = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(sql.contains("DELETE FROM events WHERE id = 'e99';") && sql.contains("INSERT INTO tpk (id, v) VALUES (2, 'b');"));

    // checksums that match: nothing to generate
    let mut same = config.clone();
    same.target_checksum_file = Some(ref_file.clone());
    let report = DataDiff::new(same).run().await.unwrap();
    assert!(report.tables.is_empty() && report.warnings.is_empty());

    // both files are required
    let mut none = config.clone();
    none.reference_checksum_file = None;
    assert!(matches!(DataDiff::new(none.clone()).run_from_checksums().await, Err(DataDiffError::Config(_))));
    none.reference_checksum_file = Some(ref_file.clone());
    none.target_checksum_file = None;
    assert!(matches!(DataDiff::new(none).run_from_checksums().await, Err(DataDiffError::Config(_))));

    let _ = fs::remove_dir_all(&out);
    pair.drop().await;
}

/// A cancelled diff stops before its first table; an estimate over the maximum stops before any
#[tokio::test]
async fn a_cancelled_or_too_long_diff_stops() {
    let Some(pair) = Pair::new("cancel", REF_SQL, TGT_SQL).await else { return };
    let out = out_dir("cancel");
    fs::create_dir_all(&out).unwrap();
    let config = pair.config(&["users"], &out).await;

    let mut dd = DataDiff::new(config.clone());
    dd.cancel();
    assert!(matches!(dd.run().await, Err(DataDiffError::Cancelled)));

    let mut sums = config.clone();
    sums.checksum_only = true;
    sums.save_reference_checksums = Some(out.join("ref.yaml"));
    DataDiff::new(sums).run().await.unwrap();
    let mut from = config.clone();
    from.reference_checksum_file = Some(out.join("ref.yaml"));
    from.target_checksum_file = Some(out.join("missing.yaml"));
    assert!(matches!(DataDiff::new(from.clone()).run().await, Err(DataDiffError::Checksum(_))));
    // a target that lacks the table: the reference's rows all differ
    let empty = ChecksumManifest { db_name: None, schema: None, created_at: String::new(), tables: vec![] };
    save_checksums(&empty, &out.join("empty.yaml")).unwrap();
    from.target_checksum_file = Some(out.join("empty.yaml"));
    let mut dd = DataDiff::new(from);
    dd.cancel();
    assert!(matches!(dd.run().await, Err(DataDiffError::Cancelled)));

    let mut slow = config.clone();
    slow.max_etc_seconds = 0.0;
    match DataDiff::new(slow.clone()).run().await {
        Err(DataDiffError::Config(msg)) => assert!(msg.starts_with("DA-E015 the estimated time, 0s (0.0h), is over the 0s allowed"), "{}", msg),
        other => panic!("{:?}", other),
    }
    slow.force = true;
    assert!(DataDiff::new(slow).run().await.is_ok(), "forced past the estimate");

    let _ = fs::remove_dir_all(&out);
    pair.drop().await;
}

// ── the parts ───────────────────────────────────────────────────────────────────────────────────────

/// The catalog reads: columns, keys, identities, tables, foreign keys
#[tokio::test]
async fn the_catalog_is_read_from_the_server() {
    let extra = "CREATE TABLE seq (id serial PRIMARY KEY, n int); CREATE SCHEMA other; CREATE TABLE other.t (k text PRIMARY KEY);";
    let Some(pair) = Pair::new("catalog", &format!("{}{}", REF_SQL, extra), TGT_SQL).await else { return };
    let (r, _t) = pair.clients().await;

    let cols = get_columns(&r, "orders").await.unwrap();
    assert_eq!(cols.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["id", "user_id", "total", "note"]);
    assert!(cols[0].is_pk && cols[0].is_identity && !cols[1].is_pk && !cols[1].is_identity);
    assert_eq!((cols[0].pg_type.clone(), cols[3].pg_type.clone(), cols[3].ordinal), (Type::INT4, Type::TEXT, 3));
    let seq = get_columns(&r, "public.seq").await.unwrap();
    assert!(seq[0].is_identity, "a serial's nextval default is an identity");
    let other = get_columns(&r, "other.t").await.unwrap();
    assert!(other[0].is_pk);
    assert!(matches!(get_columns(&r, "missing").await, Err(DataDiffError::TableNotFound(t)) if t == "missing"));
    let (pk, real) = resolve_pk_columns(&get_columns(&r, "Mixed").await.unwrap(), None);
    assert_eq!((pk.len(), real), (1, vec!["id".to_string()]));

    assert!(table_exists(&r, "users").await.unwrap());
    assert!(table_exists(&r, "other.t").await.unwrap());
    assert!(!table_exists(&r, "public.missing").await.unwrap());
    // in the server's collation: sorted here to compare
    let mut tables = list_tables(&r, "public").await.unwrap();
    tables.sort();
    assert_eq!(tables, vec!["Mixed", "events", "nopk", "only_ref", "orders", "same", "seq", "tpk", "users"]);
    let deps = get_fk_dependencies(&r, "public").await.unwrap();
    assert_eq!(deps.len(), 1);
    assert_eq!((deps[0].table.as_str(), deps[0].references_table.as_str()), ("orders", "users"));

    pair.drop().await;
}

/// A table's chunk strategy, by its size and its timestamp column, and the time it is estimated to take
#[tokio::test]
async fn a_strategy_follows_the_size_and_the_timestamp() {
    let extra = r#"
        CREATE TABLE medium (id int PRIMARY KEY, created_at date);
        CREATE TABLE big_empty (id int PRIMARY KEY, updated_at timestamp);
        CREATE TABLE big_flat (id int PRIMARY KEY, updated_at timestamp);
        INSERT INTO big_flat VALUES (1, '2024-01-01'), (2, '2024-01-01');
        CREATE TABLE big_plain (id int PRIMARY KEY, v text);
        UPDATE pg_class SET reltuples = 500000 WHERE oid = 'public.medium'::regclass;
        UPDATE pg_class SET reltuples = 2000000 WHERE relname IN ('big_empty', 'big_flat', 'big_plain');
    "#;
    let Some(pair) = Pair::new("strategy", &format!("{}{}", REF_SQL, extra), TGT_SQL).await else { return };
    let (r, t) = pair.clients().await;

    assert!(matches!(strategy_of(&r, "users", None, false).await.unwrap(), (ChunkStrategy::IdChunkSmall, None)));
    let (s, ts) = strategy_of(&r, "public.medium", None, false).await.unwrap();
    assert!(matches!(s, ChunkStrategy::IdChunkMedium) && ts.as_deref() == Some("created_at"));
    let (s, ts) = strategy_of(&r, "events", None, false).await.unwrap();
    match s {
        ChunkStrategy::Md5TimestampChunk { timestamp_column, chunk_days, .. } => {
            assert_eq!((timestamp_column.as_str(), chunk_days), ("updated_at", 1.0), "10 days in 10 chunks");
        }
        other => panic!("{:?}", other),
    }
    assert_eq!(ts.as_deref(), Some("updated_at"));
    let (s, ts) = strategy_of(&r, "big_empty", None, false).await.unwrap();
    assert!(matches!(s, ChunkStrategy::IdChunkSmall) && ts.as_deref() == Some("updated_at"), "no rows: small");
    match strategy_of(&r, "big_flat", Some("updated_at"), false).await.unwrap().0 {
        ChunkStrategy::Md5TimestampChunk { chunk_days, .. } => assert_eq!(chunk_days, 1.0, "one instant: one day"),
        other => panic!("{:?}", other),
    }
    assert!(matches!(strategy_of(&r, "big_plain", None, true).await.unwrap().0, ChunkStrategy::IdChunkLargeExcluded));
    assert!(matches!(strategy_of(&r, "big_plain", None, false).await, Err(DataDiffError::Config(_))));

    assert_eq!(get_row_count(&r, "public.events").await.unwrap(), 2_000_000);
    assert_eq!(get_row_count(&r, "big_plain").await.unwrap(), 2_000_000);
    assert!(get_timestamp_range(&r, "big_empty", "updated_at").await.unwrap().is_none());
    let (lo, hi) = get_timestamp_range(&r, "events", "updated_at").await.unwrap().unwrap();
    assert_eq!(hi.signed_duration_since(lo).num_days(), 10);

    let cols = get_columns(&r, "events").await.unwrap();
    let (md5, _) = determine_strategy(&r, "events", &cols, None, false).await.unwrap();
    assert!(estimate_table_time(&r, &t, "events", "events", &cols, &md5, 2_000_000).await.unwrap() >= 0.0);
    let small = estimate_table_time(&r, &t, "users", "users", &cols, &ChunkStrategy::IdChunkSmall, 10).await.unwrap();
    assert_eq!(small, 0.1, "a floor of 0.1s");
    let medium = estimate_table_time(&r, &t, "x", "x", &cols, &ChunkStrategy::IdChunkMedium, 200_000).await.unwrap();
    assert!((medium - 10.0).abs() < 1e-9, "50ms per 1000 rows");

    // the analysis of a whole config, and its progress
    let out = out_dir("strategy");
    let mut config = pair.config(&["users", "events", "big_plain"], &out).await;
    config.exclude_tables = ["big_plain".to_string()].into();
    let (seen, cb) = recorder();
    config.on_progress = Some(cb);
    let analyses = analyze_and_estimate(&r, &t, &config.tables, &config).await.unwrap();
    assert_eq!(analyses.len(), 3);
    assert!(matches!(analyses[2].strategy, ChunkStrategy::IdChunkLargeExcluded));
    let seen = seen.lock().unwrap().clone();
    assert!(matches!(seen.first(), Some(ProgressEvent::EstimationStart { total_tables: 3 })));
    assert!(matches!(seen.last(), Some(ProgressEvent::EstimationComplete { .. })));

    pair.drop().await;
}

/// One table pair compared by its own options: the errors it reports, the filter, identities, alternate
/// keys, excluded columns, the timestamp chunks and a cancel
#[tokio::test]
async fn a_table_diff_honours_its_options() {
    let extra = r#"CREATE TABLE narrow (id int PRIMARY KEY, a text, b text); INSERT INTO narrow VALUES (1, 'a', 'b');"#;
    let tgt_extra = r#"CREATE TABLE narrow (id int PRIMARY KEY, a text); INSERT INTO narrow VALUES (1, 'a');"#;
    let Some(pair) = Pair::new("table", &format!("{}{}", REF_SQL, extra), &format!("{}{}", TGT_SQL, tgt_extra)).await
    else {
        return;
    };
    let (r, t) = pair.clients().await;

    // the events' analysis: the md5 timestamp path
    let cols = get_columns(&r, "events").await.unwrap();
    let (md5, ts) = determine_strategy(&r, "events", &cols, None, false).await.unwrap();
    let analysis = TableAnalysis {
        ref_table: "events".into(),
        target_table: "events".into(),
        row_count: 2_000_000,
        timestamp_column: ts,
        strategy: md5,
        estimated_seconds: 0.0,
    };

    // the setup's errors, row by row and by timestamp
    for a in [None, Some(analysis.clone())] {
        let err = |rt: &'static str, tt: &'static str| table_err(&r, &t, rt, tt, a.clone());
        assert!(matches!(err("missing", "users").await, DataDiffError::TableNotFound(x) if x == "missing"));
        assert!(matches!(err("users", "missing").await, DataDiffError::TableNotFound(x) if x == "missing"));
        assert!(matches!(err("narrow", "narrow").await, DataDiffError::ColumnMismatch { .. }));
        assert!(matches!(err("nopk", "nopk").await, DataDiffError::NoPrimaryKey(x) if x == "nopk"));
    }

    // row by row, one row a chunk, a filter with and without its WHERE
    let mut d = TableDataDiff::new(&r, &t, "users".into(), "public.users".into());
    d.set_chunk_size(0);
    d.set_table_where(Some("  WHERE id >= 3".into()));
    d.set_columns_to_ignore(["SCORE".to_string()].into());
    assert_eq!(run_table(&mut d).await.unwrap().0, 1, "3 only: 4 and 5 differ by score alone");
    d.set_table_where(Some("id < 3".into()));
    let (ins, upd, _, upd_sql) = run_table(&mut d).await.unwrap();
    assert_eq!((ins, upd), (0, 1));
    assert!(upd_sql.contains("-- Data diff for table: users"));

    // identities: left out by default and when asked, written when asked
    let mut d = TableDataDiff::new(&r, &t, "orders".into(), "orders".into());
    assert!(run_table(&mut d).await.unwrap().2.contains("INSERT INTO orders (user_id, total, note) VALUES"));
    d.set_include_identity_columns(Some(true));
    let with_id = run_table(&mut d).await.unwrap().2;
    assert!(with_id.contains("INSERT INTO orders (id, user_id, total, note) OVERRIDING SYSTEM VALUE VALUES (2, 2, 20, 'n2');"), "{}", with_id);
    d.set_include_identity_columns(Some(false));
    assert!(run_table(&mut d).await.unwrap().2.contains("INSERT INTO orders (user_id, total, note) VALUES"));
    // a GENERATED ALWAYS identity takes the reference's value, and the row is then the same
    t.batch_execute(&with_id).await.expect("an identity written OVERRIDING SYSTEM VALUE applies");
    d.set_include_identity_columns(Some(true));
    assert_eq!(run_table(&mut d).await.unwrap().0, 0);

    // an alternate key, the real one left out, an excluded column
    let mut d = TableDataDiff::new(&r, &t, "users".into(), "users".into());
    d.set_alternate_keys(Some(vec!["email".into()]));
    d.set_exclude_real_pk(true);
    d.set_exclude_columns(["Score".to_string()].into());
    let (ins, _, ins_sql, _) = run_table(&mut d).await.unwrap();
    assert_eq!(ins, 1);
    assert!(ins_sql.contains("INSERT INTO users (name, email, active, updated_at) VALUES ('c', 'c@x', FALSE,"), "{}", ins_sql);

    // by timestamp chunks: the changed and the missing rows only, a chunk at a time
    let (seen, cb) = recorder();
    let mut d = TableDataDiff::new(&r, &t, "events".into(), "events".into());
    d.set_analysis(Some(analysis.clone()));
    d.set_on_progress(Some(cb));
    d.set_chunk_size(1);
    let (ins, upd, ins_sql, upd_sql) = run_table(&mut d).await.unwrap();
    assert_eq!((ins, upd), (1, 1));
    assert!(ins_sql.contains("'e3'") && upd_sql.contains("WHERE id = 'e2';"));
    assert_eq!(seen.lock().unwrap().len(), 10, "one progress event a chunk");

    // a cancelled diff stops at its first row, or its first chunk
    for a in [None, Some(analysis)] {
        let mut d = TableDataDiff::new(&r, &t, "events".into(), "events".into());
        d.set_analysis(a);
        d.cancel();
        assert!(matches!(run_table(&mut d).await, Err(DataDiffError::Cancelled)));
    }

    pair.drop().await;
}

/// The delete phase: the target's rows the reference lacks, by the key or an alternate one
#[tokio::test]
async fn the_delete_sync_finds_the_targets_extra_rows() {
    let tgt_extra = "INSERT INTO users (id, name, email) VALUES (7, 'g', NULL), (8, 'h', 'h@x');";
    let Some(pair) = Pair::new("delete", REF_SQL, &format!("{}{}", TGT_SQL, tgt_extra)).await else { return };
    let (r, t) = pair.clients().await;

    let mut s = TableDeleteSync::new(&t, &r, "users".into(), "users".into());
    s.set_chunk_size(2);
    let (n, sql) = run_delete(&mut s).await.unwrap();
    assert_eq!(n, 3);
    assert!(sql.starts_with("-- -") && sql.contains("-- DELETE statements for table: users"));
    assert!(sql.contains("DELETE FROM users WHERE id = 6;") && sql.contains("DELETE FROM users WHERE id = 8;"));

    // nothing extra: every chunk counts the same
    let mut s = TableDeleteSync::new(&t, &r, "same".into(), "same".into());
    assert_eq!(run_delete(&mut s).await.unwrap().0, 0);

    // by an alternate key, a NULL one among them
    let mut s = TableDeleteSync::new(&t, &r, "users".into(), "users".into());
    s.set_alternate_keys(Some(vec!["email".into()]));
    s.set_chunk_size(0);
    let (n, sql) = run_delete(&mut s).await.unwrap();
    assert_eq!(n, 2, "f@x and h@x: a NULL email the reference also has is no delete");
    assert!(sql.contains("DELETE FROM users WHERE email = 'f@x';"));

    let mut s = TableDeleteSync::new(&t, &r, "tpk".into(), "tpk".into());
    assert!(matches!(run_delete(&mut s).await, Err(DataDiffError::NoPrimaryKey(x)) if x == "tpk"));
    let mut s = TableDeleteSync::new(&t, &r, "users".into(), "users".into());
    s.set_alternate_keys(Some(vec!["extra".into()]));
    assert!(matches!(run_delete(&mut s).await, Err(DataDiffError::ColumnMismatch { .. })), "the reference lacks the key");
    let mut s = TableDeleteSync::new(&t, &r, "users".into(), "users".into());
    s.cancel();
    assert!(matches!(run_delete(&mut s).await, Err(DataDiffError::Cancelled)));

    pair.drop().await;
}

/// The checksums of one table: one chunk of all its rows, or one per timestamp chunk
#[tokio::test]
async fn a_tables_checksums_follow_its_strategy() {
    let extra = "CREATE TABLE two_keys (tenant int, code uuid, v text, PRIMARY KEY (tenant, code)); \
        INSERT INTO two_keys VALUES (1, '00000000-0000-0000-0000-000000000001', 'a'), (2, '00000000-0000-0000-0000-000000000002', 'b');";
    let Some(pair) = Pair::new("sums", &format!("{}{}", REF_SQL, extra), TGT_SQL).await else { return };
    let (r, _t) = pair.clients().await;
    let cols = get_columns(&r, "events").await.unwrap();
    let (pk, _) = resolve_pk_columns(&cols, None);
    let (md5, ts) = determine_strategy(&r, "events", &cols, None, false).await.unwrap();
    let analysis = TableAnalysis {
        ref_table: "public.events".into(),
        target_table: "public.events".into(),
        row_count: 11,
        timestamp_column: ts,
        strategy: md5,
        estimated_seconds: 0.0,
    };
    let sums = compute_table_checksums(&r, "public.events", &cols, &pk, &analysis).await.unwrap();
    assert_eq!(sums.table_name, "events");
    assert_eq!(sums.chunks.len(), 10);
    assert_eq!(sums.chunks.iter().map(|c| c.row_hashes.len()).sum::<usize>(), 11);
    assert_eq!(sums.chunks[9].row_hashes.len(), 2, "the last chunk takes the last instant");
    assert!(sums.chunks[0].row_hashes.contains_key("e1"));

    let small = TableAnalysis { strategy: ChunkStrategy::IdChunkSmall, timestamp_column: None, ..analysis.clone() };
    let sums = compute_table_checksums(&r, "events", &cols, &pk, &small).await.unwrap();
    assert_eq!((sums.chunks.len(), sums.chunks[0].row_hashes.len()), (1, 11));
    assert_eq!(sums.chunks[0].chunk_start, "");

    // any key's type, one column or more, keys a row by its text
    let two = get_columns(&r, "two_keys").await.unwrap();
    let (two_pk, _) = resolve_pk_columns(&two, None);
    let sums = compute_table_checksums(&r, "two_keys", &two, &two_pk, &small).await.unwrap();
    let mut keys: Vec<&String> = sums.chunks[0].row_hashes.keys().collect();
    keys.sort();
    assert_eq!(keys, vec!["1|00000000-0000-0000-0000-000000000001", "2|00000000-0000-0000-0000-000000000002"]);
    // no key: refused, not a query that cannot parse
    let none = get_columns(&r, "nopk").await.unwrap();
    assert!(matches!(
        compute_table_checksums(&r, "nopk", &none, &[], &small).await,
        Err(DataDiffError::NoPrimaryKey(t)) if t == "nopk"
    ));

    // a table without an analysis is left out
    let tables = vec![
        ("public.events".to_string(), cols.clone(), pk.clone()),
        ("other".to_string(), cols.clone(), pk.clone()),
    ];
    let manifest = compute_db_checksums(&r, &tables, &[analysis], Some("db"), None).await.unwrap();
    assert_eq!(manifest.tables.len(), 1);
    assert_eq!((manifest.db_name.as_deref(), manifest.schema.as_deref()), (Some("db"), None));

    pair.drop().await;
}

/// A row's values, decoded by each column's type; NULL in any type
#[tokio::test]
async fn a_rows_values_are_decoded_by_their_types() {
    let Some(pair) = Pair::new("types", "", "").await else { return };
    let (r, _t) = pair.clients().await;
    let types = [
        ("bool", Type::BOOL), ("int2", Type::INT2), ("int4", Type::INT4), ("int8", Type::INT8),
        ("float4", Type::FLOAT4), ("float8", Type::FLOAT8), ("text", Type::TEXT), ("varchar", Type::VARCHAR),
        ("char(2)", Type::BPCHAR), ("name", Type::NAME), ("bytea", Type::BYTEA), ("timestamp", Type::TIMESTAMP),
        ("timestamptz", Type::TIMESTAMPTZ), ("date", Type::DATE), ("time", Type::TIME), ("uuid", Type::UUID),
        ("json", Type::JSON), ("jsonb", Type::JSONB), ("numeric", Type::NUMERIC), ("oid", Type::OID),
        // a type the match does not name: read as text when it is text, else NULL
        ("text", Type::INTERVAL), ("int4[]", Type::INTERVAL),
    ];
    let values = [
        "true", "1", "2", "3", "1.5", "2.5", "'txt'", "'vc'", "'bp'", "'nm'", "'\\x0102'", "'2024-01-02 03:04:05'",
        "'2024-01-02 03:04:05+00'", "'2024-01-02'", "'03:04:05'", "'00000000-0000-0000-0000-000000000001'",
        "'{\"a\":1}'", "'{\"b\":2}'", "1.25", "4000000000", "'fallback'", "'{1,2}'",
    ];
    let cols: Vec<ColumnInfo> = types
        .iter()
        .enumerate()
        .map(|(i, (_, ty))| ColumnInfo { name: format!("c{}", i), pg_type: ty.clone(), is_pk: false, is_identity: false, identity_always: false, ordinal: i })
        .collect();
    let select = |nulls: bool| {
        let list: Vec<String> = types
            .iter()
            .zip(values)
            .map(|((sql_ty, ty), v)| {
                let value = if nulls { format!("NULL::{}", sql_ty) } else { format!("{}::{}", v, sql_ty) };
                // NUMERIC as the diff's queries select it: its exact text
                if *ty == Type::NUMERIC { format!("({})::text", value) } else { value }
            })
            .collect();
        format!("SELECT {}", list.join(", "))
    };

    let row = r.query_one(select(false).as_str(), &[]).await.unwrap();
    let got = extract_row(&row, &cols);
    let ts = chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap().and_hms_opt(3, 4, 5).unwrap();
    let expected = [
        RowValue::Bool(true), RowValue::I16(1), RowValue::I32(2), RowValue::I64(3), RowValue::F32(1.5),
        RowValue::F64(2.5), RowValue::Text("txt".into()), RowValue::Text("vc".into()), RowValue::Text("bp".into()),
        RowValue::Text("nm".into()), RowValue::Bytes(vec![1, 2]), RowValue::Timestamp(ts),
        RowValue::TimestampTz(ts.and_utc()), RowValue::Date(ts.date()), RowValue::Time(ts.time()),
        RowValue::Uuid(uuid::Uuid::from_u128(1)), RowValue::Json(serde_json::json!({"a": 1})),
        RowValue::Json(serde_json::json!({"b": 2})),
    ];
    assert_eq!(&got[..expected.len()], &expected[..]);
    assert_eq!(got[18], RowValue::Numeric("1.25".into()));
    assert_eq!(got[19], RowValue::I64(4_000_000_000), "an oid is unsigned");
    assert_eq!(got[20], RowValue::Text("fallback".into()));
    assert_eq!(got[21], RowValue::Null);
    assert_eq!(extract_row_value(&row, 2, &Type::INT4), RowValue::I32(2));

    let row = r.query_one(select(true).as_str(), &[]).await.unwrap();
    assert!(extract_row(&row, &cols).iter().all(|v| *v == RowValue::Null));

    pair.drop().await;
}

/// Every column type round-trips: numeric (NaN too), arrays, intervals, enums and oids are read as what
/// they hold, so a difference in any is found - row by row, and in timestamp chunks of a large table with
/// an integer key - and the SQL generated, applied to the target, leaves nothing to diff
#[tokio::test]
async fn every_type_round_trips_through_a_diff() {
    let typed = "CREATE TYPE mood AS ENUM ('ok', 'sad'); \
        CREATE TABLE typed (id int PRIMARY KEY, amount numeric(10,2), big numeric, tags int[], span interval, \
            m mood, o oid, updated_at timestamp NOT NULL);";
    let ref_sql = format!("{} INSERT INTO typed VALUES \
        (1, 10.50, 12345678901234567890.123456789, '{{1,2}}', '1 day', 'ok', 7, '2024-01-01'), \
        (2, 20.00, 'NaN', '{{3}}', '2 hours', 'sad', 4000000000, '2024-01-02'), \
        (3, 30.25, 0, NULL, NULL, NULL, NULL, '2024-01-03'), \
        (4, -1.5, 1, '{{}}', '0', 'ok', 1, '2024-01-04'); \
        UPDATE pg_class SET reltuples = 2000000 WHERE oid = 'public.typed'::regclass;", typed);
    let tgt_sql = format!("{} INSERT INTO typed VALUES \
        (1, 99.99, 12345678901234567890.123456789, '{{1,2}}', '1 day', 'ok', 7, '2024-01-01'), \
        (2, 20.00, 'NaN', '{{3,4}}', '2 hours', 'ok', 4000000000, '2024-01-02'), \
        (4, -1.5, 1, '{{}}', '0', 'ok', 1, '2024-01-04'), \
        (5, 5, 5, '{{5}}', '5 days', 'sad', 5, '2024-01-05');", typed);
    let Some(pair) = Pair::new("typed", &ref_sql, &tgt_sql).await else { return };
    let (r, t) = pair.clients().await;
    let cols = get_columns(&r, "typed").await.unwrap();
    let (md5, ts) = determine_strategy(&r, "typed", &cols, None, false).await.unwrap();
    assert!(matches!(md5, ChunkStrategy::Md5TimestampChunk { .. }));
    let analysis = TableAnalysis {
        ref_table: "typed".into(),
        target_table: "typed".into(),
        row_count: 2_000_000,
        timestamp_column: ts,
        strategy: md5,
        estimated_seconds: 0.0,
    };

    // by timestamp chunks on an integer key: the changed rows and the missing one
    let mut d = TableDataDiff::new(&r, &t, "typed".into(), "typed".into());
    d.set_analysis(Some(analysis.clone()));
    let (ins, upd, ins_md5, upd_md5) = run_table(&mut d).await.unwrap();
    assert_eq!((ins, upd), (1, 2), "{}{}", ins_md5, upd_md5);

    // row by row: the same SQL
    let mut d = TableDataDiff::new(&r, &t, "typed".into(), "typed".into());
    let (ins, upd, ins_sql, upd_sql) = run_table(&mut d).await.unwrap();
    assert_eq!((ins, upd), (1, 2));
    assert_eq!((&ins_sql, &upd_sql), (&ins_md5, &upd_md5));
    assert!(ins_sql.contains("VALUES (3, 30.25, 0, NULL, NULL, NULL, NULL, '2024-01-03 00:00:00'::timestamp);"), "{}", ins_sql);
    assert!(upd_sql.contains("UPDATE typed SET amount = 10.50 WHERE id = 1;"), "{}", upd_sql);
    assert!(upd_sql.contains("UPDATE typed SET tags = '{3}', m = 'sad' WHERE id = 2;"), "{}", upd_sql);

    let mut s = TableDeleteSync::new(&t, &r, "typed".into(), "typed".into());
    let (n, del_sql) = run_delete(&mut s).await.unwrap();
    assert_eq!(n, 1);

    // applied, the target is the reference's: nothing left to diff either way
    t.batch_execute(&format!("{}{}{}", ins_sql, upd_sql, del_sql)).await.expect("the generated SQL applies");
    for a in [None, Some(analysis)] {
        let mut d = TableDataDiff::new(&r, &t, "typed".into(), "typed".into());
        d.set_analysis(a);
        assert_eq!(run_table(&mut d).await.unwrap().0 + run_table(&mut d).await.unwrap().1, 0);
    }
    let same: i64 = t
        .query_one("SELECT count(*) FROM typed WHERE id = 2 AND big = 'NaN' AND o = 4000000000 AND tags = '{3}'", &[])
        .await.unwrap().get(0);
    assert_eq!(same, 1);
    assert_eq!(run_delete(&mut TableDeleteSync::new(&t, &r, "typed".into(), "typed".into())).await.unwrap().0, 0);

    pair.drop().await;
}

/// No tables named: every table of the reference schema, parents before children by their foreign keys,
/// into one file - every table's INSERTs and UPDATEs, then the DELETEs, children first - that applies to
/// the target as it is (its foreign keys too), after which nothing differs. A filter takes the reference's
/// rows only
#[tokio::test]
async fn a_schema_is_compared_whole_by_its_foreign_keys_into_one_file_that_applies() {
    // an order of the target refers to its user 6, which the reference lacks: its delete goes first
    let tgt_extra = "INSERT INTO orders (user_id, total, note) VALUES (6, 60, 'n6'), (6, 61, 'n6b');";
    let Some(pair) = Pair::new("schema", REF_SQL, &format!("{}{}", TGT_SQL, tgt_extra)).await else { return };
    let (r, t) = pair.clients().await;
    let out = out_dir("schema");
    let mut config = pair.config(&[], &out).await;
    config.reference_schema = Some("public".into());
    config.target_schema = Some("public".into());
    config.single_file = true;
    config.output_file = out.join("all.sql");
    let (seen, cb) = recorder();
    config.on_progress = Some(cb);

    let report = DataDiff::new(config.clone()).run().await.expect("the diff of the schema");
    let names: Vec<&str> = report.tables.iter().map(|t| t.table_name.as_str()).collect();
    let pos = |n: &str| names.iter().position(|x| *x == n).unwrap_or_else(|| panic!("{} in {:?}", n, names));
    assert_eq!(names.len(), 8, "every table of public: {:?}", names);
    assert!(pos("users") < pos("orders"), "a parent before its child: {:?}", names);
    assert_eq!(counts(&report, "orders"), (0, 1, 1));
    assert_eq!(report.master_script.as_deref(), Some(out.join("all.sql").to_str().unwrap()));
    assert!(report.warnings.contains(&"DA-E005 table not found: public.only_ref".to_string()), "{:?}", report.warnings);
    assert!(seen.lock().unwrap().iter().any(|e| matches!(e, ProgressEvent::SortingDependencies)));

    let sql = read(&out, "all.sql");
    assert_eq!(fs::read_dir(&out).unwrap().count(), 1, "the one file, no other");
    let (child, parent) = (sql.find("DELETE FROM public.orders").unwrap(), sql.find("DELETE FROM public.users").unwrap());
    assert!(child < parent, "children's deletes first: {}", sql);
    assert!(sql.rfind("UPDATE ").unwrap() < sql.find("DELETE FROM").unwrap());

    // applied, the target holds the reference's rows: nothing differs
    t.batch_execute(&sql).await.expect("the one file applies, foreign keys and all");
    let again = DataDiff::new(config.clone()).run().await.unwrap();
    assert!(again.tables.iter().all(|t| (t.inserts, t.updates, t.deletes) == (0, 0, 0)), "{:?}", again.tables);

    // unordered, as listed: the child's table where the schema listing has it
    let mut unordered = config.clone();
    unordered.check_dependencies = false;
    unordered.output_file = out.join("unordered.sql");
    let listed = DataDiff::new(unordered).run().await.unwrap();
    let names: Vec<&str> = listed.tables.iter().map(|t| t.table_name.as_str()).collect();
    assert!(names.iter().position(|n| *n == "orders") < names.iter().position(|n| *n == "users"), "{:?}", names);

    // a filter: the reference's rows it takes, here none of users' changes but 3
    r.batch_execute("INSERT INTO users VALUES (7, 'g', 'g@x', true, 7.0, '2024-01-07')").await.unwrap();
    let mut filtered = pair.config(&["users"], &out).await;
    filtered.table_where = Some("WHERE id <> 7".into());
    filtered.output_stream = Some(Arc::new(Mutex::new(Vec::<u8>::new())));
    assert_eq!(counts(&DataDiff::new(filtered.clone()).run().await.unwrap(), "users"), (0, 0, 0), "7 is filtered out");
    filtered.table_where = None;
    assert_eq!(counts(&DataDiff::new(filtered).run().await.unwrap(), "users").0, 1, "and in without the filter");

    let _ = fs::remove_dir_all(&out);
    pair.drop().await;
}

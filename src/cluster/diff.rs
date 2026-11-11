//! One table of one node made the same as a primary's, row by row - YT Cluster's ytctl data diff (its
//! `tui/data_diff.rs`), the second-class side of this module, on the first-class side's conventions
//! ([`super::recon`], ytserv's, 2026-11-11):
//!
//! - a row is identified as ytserv identifies it: its key columns as text joined by `'|'` ([`Shape::id_expr`],
//!   the same id as [`super::recon::pk_expr`]). Two rows of one node that render one id (a text key holding
//!   `'|'`, a float key) refuse the table, never a repair by a guess ([`ids_of`]); a row is deleted and
//!   upserted by its key columns from its whole row, never by the id's text;
//! - a row is compared by ytserv's basis, `to_jsonb(t.*)` less the ignored and the generated columns, as
//!   the md5 of it in full - a repair writes on a difference, so the row's hash is not cut to 32 bits;
//! - a table, and a range of its first key, fold as ytserv's frames fold: `(count, xor32, sum64)` ([`Fold`]).
//!
//! Phase 1 ([`table_fold_sql`]) finds the nodes whose table differs; phase 2 keys each node's rows, plans
//! the primary's rows that differ or are missing there as upserts and the node's rows the primary lacks as
//! deletes ([`Plan`]), in one SQL file per node ([`repair_sql`]) - in YT Cluster under `yt.node = 'local'`,
//! the guard its sync trigger honours, so a repair reaches no other node. A table over [`LARGE_ROWS`] is
//! folded by key range, a differing range halved until it is small ([`step`]), and only the small ones read
//! row by row; a one-key range with a second key column is read in pages ([`page_cut`]).

use std::collections::BTreeMap;

use tokio_postgres::Client;

use super::recon::SUM64_WRAPPED;

/// What a table compares and repairs: the columns compared by no node and updated on none, and the
/// filter that takes the rows compared (YT Cluster's `ignore_col` and `filter`)
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TableRules {
    pub ignore: Vec<String>,
    pub filter: Option<String>,
}

impl TableRules {
    /// From a comma-separated column list and a filter, as YT Cluster's table config writes them
    pub fn new(ignore_col: Option<&str>, filter: Option<&str>) -> Self {
        TableRules {
            ignore: ignore_col.unwrap_or("").split(',').map(str::trim).filter(|c| !c.is_empty()).map(str::to_string).collect(),
            filter: filter.map(str::trim).filter(|f| !f.is_empty()).map(str::to_string),
        }
    }
}

/// An identifier, always double-quoted, a quote doubled
pub fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// A SQL literal: `'` doubled (standard_conforming_strings, the default)
pub fn lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Every connection the diff reads on: text forms that do not depend on the session - a timestamptz in
/// UTC, a float in full - so two nodes set up otherwise hash the same row alike
pub const SESSION_SQL: &str = "SET TimeZone = 'UTC'; SET extra_float_digits = 1";

/// YT Cluster's guard for a repair: the sync trigger replicates nothing written under it
pub const YT_LOCAL_GUARD: &str = "SET LOCAL yt.node = 'local';";

/// A PostgreSQL error with what the server said: `[SQLSTATE] message`, its detail and hint
pub fn pg_text(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => {
            let mut msg = format!("[{}] {}", db.code().code(), db.message());
            if let Some(detail) = db.detail() {
                msg.push_str(&format!(" | detail: {}", detail));
            }
            if let Some(hint) = db.hint() {
                msg.push_str(&format!(" | hint: {}", hint));
            }
            msg
        }
        None => e.to_string(),
    }
}

/// One column of the table, as the diff writes it
#[derive(Debug, Clone, PartialEq)]
pub struct Col {
    pub name: String,
    /// GENERATED ... STORED: never written
    pub generated: bool,
    /// GENERATED ALWAYS AS IDENTITY: written with OVERRIDING SYSTEM VALUE, never updated
    pub identity_always: bool,
}

/// What the diff knows of a table: its key, its columns, what it leaves out
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Shape {
    pub pk: Vec<String>,
    pub cols: Vec<Col>,
    /// compared by no node, updated on none
    pub ignored: Vec<String>,
    /// only the rows it takes are compared
    pub filter: Option<String>,
}

/// The table's primary key, in key order
pub const PK_SQL: &str = "SELECT a.attname::text FROM pg_index i \
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey) \
    WHERE i.indrelid = $1::text::regclass AND i.indisprimary ORDER BY array_position(i.indkey::int2[], a.attnum)";

/// Its generated columns: phase 1 leaves them out, as phase 2 does - a repair cannot write them
pub const GENERATED_SQL: &str = "SELECT attname::text FROM pg_attribute \
    WHERE attrelid = $1::text::regclass AND attnum > 0 AND NOT attisdropped AND attgenerated <> '' ORDER BY attnum";

/// Its columns: name, generated, identity always
pub const COLS_SQL: &str = "SELECT attname::text, attgenerated <> '', attidentity = 'a' FROM pg_attribute \
    WHERE attrelid = $1::text::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum";

/// The first key column's type, by name: ranges are cut on an integer only
pub const KEY_TYPE_SQL: &str = "SELECT atttypid::regtype::text FROM pg_attribute \
    WHERE attrelid = $1::text::regclass AND attname = $2 AND NOT attisdropped";

/// What a row is compared by, as SQL over `t`: ytserv's basis - `to_jsonb(t.*)` less these columns - with
/// any name quoted as a literal (ytserv's [`super::recon::row_basis`] gives the same for plain names)
pub fn basis(out: &[String]) -> String {
    if out.is_empty() {
        "to_jsonb(t.*)::text".to_string()
    } else {
        format!("(to_jsonb(t.*) - ARRAY[{}]::text[])::text", out.iter().map(|c| lit(c)).collect::<Vec<_>>().join(","))
    }
}

/// A table's or a range's rows folded as ytserv folds a frame: their count, the xor of their crc32 and
/// the wrapped sum of their 60-bit hashes, over [`basis`]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Fold {
    pub count: i64,
    pub xor32: u32,
    pub sum64: i64,
}

impl Fold {
    /// One 64-bit value of both folds, for a report that keeps a (count, checksum) pair: equal folds give
    /// equal values; two different folds giving one is a 64-bit collision
    pub fn signature(&self) -> i64 {
        ((self.sum64 as u64).rotate_left(32) ^ self.xor32 as u64) as i64
    }
}

/// The fold's select list over a CTE `b` of `c32` and `c60` (ytserv's)
fn fold_select() -> String {
    format!("coalesce(bit_xor(c32), 0)::bigint, {}", SUM64_WRAPPED)
}

fn fold_cols(basis: &str) -> String {
    format!("('x' || md5({b}))::bit(32)::int AS c32, ('x' || substr(md5({b}), 1, 15))::bit(60)::bigint AS c60", b = basis)
}

fn table_of(schema: &str, table: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(table))
}

fn where_filter(filter: Option<&str>) -> String {
    filter.map(str::trim).filter(|f| !f.is_empty()).map(|f| format!(" WHERE ({})", f)).unwrap_or_default()
}

/// Phase 1: the table's fold over what phase 2 compares - the filter's rows, the ignored and the `generated`
/// columns left out (review 2026-11-11: with the generated ones in, a node whose only difference was one was
/// found by phase 1 and planned nothing by phase 2)
pub fn table_fold_sql(schema: &str, table: &str, rules: &TableRules, generated: &[String]) -> String {
    let out: Vec<String> = rules.ignore.iter().chain(generated.iter()).cloned().collect();
    format!("WITH b AS (SELECT {} FROM {} t{}) SELECT count(*)::bigint, {} FROM b",
        fold_cols(&basis(&out)), table_of(schema, table), where_filter(rules.filter.as_deref()), fold_select())
}

impl Shape {
    /// From the primary's catalog rows and the table's rules
    pub fn new(pk: Vec<String>, cols: Vec<Col>, rules: &TableRules) -> Result<Self, String> {
        if pk.is_empty() {
            return Err(tr!("error.cluster_no_key"));
        }
        if let Some(k) = pk.iter().find(|k| rules.ignore.contains(k)) {
            return Err(tr!("error.cluster_key_ignored", column = k));
        }
        Ok(Shape { pk, cols, ignored: rules.ignore.clone(), filter: rules.filter.clone() })
    }

    /// The row's id as ytserv renders it: its key columns as text, joined by `'|'` (quoted names)
    pub fn id_expr(&self) -> String {
        if self.pk.len() == 1 {
            format!("(t.{}::text)", quote_ident(&self.pk[0]))
        } else {
            format!("concat_ws('|', {})", self.pk.iter().map(|k| format!("t.{}::text", quote_ident(k))).collect::<Vec<_>>().join(", "))
        }
    }

    /// What a row is compared by: every column but the ignored and the generated ones
    fn left_out(&self) -> Vec<String> {
        self.ignored.iter().cloned().chain(self.cols.iter().filter(|c| c.generated).map(|c| c.name.clone())).collect()
    }

    /// The first key column, the one ranges are cut on
    pub fn range_col(&self) -> &str {
        self.pk.first().map_or("", String::as_str)
    }

    /// The WHERE over `t`: the filter, and the range when given
    fn where_clause(&self, range: Option<KeyRange>) -> String {
        let mut conds: Vec<String> = Vec::new();
        if let Some(f) = self.filter.as_deref() {
            conds.push(format!("({})", f));
        }
        if let Some(r) = range {
            conds.push(format!("({})", range_sql(self.range_col(), r)));
        }
        if conds.is_empty() { String::new() } else { format!(" WHERE {}", conds.join(" AND ")) }
    }

    /// Each row's id and the md5 of what is compared of it
    pub fn key_md5_sql(&self, schema: &str, table: &str) -> String {
        self.key_md5_range_sql(schema, table, None)
    }

    /// The same over one range of the first key column (None: the whole table), the filter kept
    pub fn key_md5_range_sql(&self, schema: &str, table: &str, range: Option<KeyRange>) -> String {
        format!("SELECT {}, md5({}) FROM {} t{}", self.id_expr(), basis(&self.left_out()), table_of(schema, table), self.where_clause(range))
    }

    /// One page of a range's ids and hashes: the ids after `after`, in byte order (`COLLATE "C"`, the
    /// order the maps keep, as ytserv pages its rows), at most `limit` of them
    pub fn key_md5_page_sql(&self, schema: &str, table: &str, range: KeyRange, after: Option<&str>, limit: usize) -> String {
        let id = format!("{} COLLATE \"C\"", self.id_expr());
        let mut conds: Vec<String> = Vec::new();
        if let Some(f) = self.filter.as_deref() {
            conds.push(format!("({})", f));
        }
        conds.push(format!("({})", range_sql(self.range_col(), range)));
        if let Some(a) = after {
            conds.push(format!("{} > {}", id, lit(a)));
        }
        format!("SELECT {}, md5({}) FROM {} t WHERE {} ORDER BY {} LIMIT {}",
            self.id_expr(), basis(&self.left_out()), table_of(schema, table), conds.join(" AND "), id, limit)
    }

    /// Each range's fold in one statement: the ranges are a VALUES list joined to the table, so a range
    /// with no rows folds to `(0, 0, 0)`
    pub fn range_fold_sql(&self, schema: &str, table: &str, ranges: &[KeyRange]) -> String {
        let values = ranges.iter().enumerate()
            .map(|(i, r)| format!("({}, {}, {})", i, lo_lit(r.lo), bound_lit(r.hi)))
            .collect::<Vec<_>>().join(", ");
        let c = format!("t.{}", quote_ident(self.range_col()));
        let filter = self.filter.as_deref().map(|f| format!(" AND ({})", f)).unwrap_or_default();
        format!("WITH b AS (SELECT r.i, {c} AS k, {cols} FROM (VALUES {values}) AS r(i, lo, hi) \
            LEFT JOIN {rel} t ON {c} >= r.lo AND (r.hi IS NULL OR {c} < r.hi){filter}) \
            SELECT i, count(k)::bigint, {fold} FROM b GROUP BY i ORDER BY i",
            c = c, cols = fold_cols(&basis(&self.left_out())), values = values, rel = table_of(schema, table),
            filter = filter, fold = fold_select())
    }

    /// The rows of these ids ($1, text[]), whole
    pub fn rows_sql(&self, schema: &str, table: &str) -> String {
        format!("SELECT {k}, to_jsonb(t)::text FROM {} t WHERE {k} = ANY ($1::text[])", table_of(schema, table), k = self.id_expr())
    }

    /// The node made to hold the primary's row: inserted, or its compared columns set - an ignored column
    /// there stays the node's
    pub fn upsert_sql(&self, schema: &str, table: &str, row_json: &str) -> String {
        let cols: Vec<&Col> = self.cols.iter().filter(|c| !c.generated).collect();
        let names = cols.iter().map(|c| quote_ident(&c.name)).collect::<Vec<_>>().join(", ");
        let overriding = if cols.iter().any(|c| c.identity_always) { " OVERRIDING SYSTEM VALUE" } else { "" };
        let set: Vec<String> = cols.iter()
            .filter(|c| !self.pk.contains(&c.name) && !self.ignored.contains(&c.name) && !c.identity_always)
            .map(|c| format!("{0} = EXCLUDED.{0}", quote_ident(&c.name))).collect();
        let action = if set.is_empty() { "DO NOTHING".to_string() } else { format!("DO UPDATE SET {}", set.join(", ")) };
        format!("INSERT INTO {t} ({n}){o} SELECT {n} FROM jsonb_populate_record(NULL::{t}, {j}::jsonb) ON CONFLICT ({k}) {a};",
            t = table_of(schema, table), n = names, o = overriding, j = lit(row_json),
            k = self.pk.iter().map(|k| quote_ident(k)).collect::<Vec<_>>().join(", "), a = action)
    }

    /// The node's row the primary does not have, gone: by its key columns, read from the node's own row
    /// (its whole row's JSON, or one of its key columns only), never by the id's text
    pub fn delete_sql(&self, schema: &str, table: &str, row_json: &str) -> String {
        let k = self.pk.iter().map(|k| quote_ident(k)).collect::<Vec<_>>().join(", ");
        format!("DELETE FROM {t} WHERE ({k}) IN (SELECT {k} FROM jsonb_populate_record(NULL::{t}, {j}::jsonb));",
            t = table_of(schema, table), k = k, j = lit(row_json))
    }
}

/// The ids and hashes of a query's rows, as a map: two rows of one id refuse the table - their id is
/// ambiguous (a text key holding `'|'`, a float key), and a repair by it would be a guess
pub fn ids_of(rows: impl IntoIterator<Item = (String, String)>) -> Result<BTreeMap<String, String>, String> {
    let mut out = BTreeMap::new();
    for (id, v) in rows {
        if out.insert(id.clone(), v).is_some() {
            return Err(tr!("error.cluster_ambiguous_id", id = id));
        }
    }
    Ok(out)
}

/// What the node needs: the ids to upsert (missing there, or different), the ids to delete (there, not
/// on the primary)
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Plan {
    pub upsert: Vec<String>,
    pub delete: Vec<String>,
}

impl Plan {
    pub fn of(primary: &BTreeMap<String, String>, node: &BTreeMap<String, String>) -> Self {
        let mut p = Plan::default();
        for (k, h) in primary {
            if node.get(k) != Some(h) {
                p.upsert.push(k.clone());
            }
        }
        p.delete = node.keys().filter(|k| !primary.contains_key(*k)).cloned().collect();
        p
    }

    pub fn is_empty(&self) -> bool {
        self.upsert.is_empty() && self.delete.is_empty()
    }

    /// The plans of disjoint key ranges, as one
    pub fn merge(parts: Vec<Plan>) -> Self {
        let mut out = Plan::default();
        for p in parts {
            out.upsert.extend(p.upsert);
            out.delete.extend(p.delete);
        }
        out
    }
}

/// The SQL file for one node: the header as comments, then one transaction - `guard` first (YT Cluster:
/// [`YT_LOCAL_GUARD`]), then the statements
pub fn repair_sql(header: &str, guard: Option<&str>, statements: &[String]) -> String {
    let mut s = String::new();
    for l in header.lines() {
        s.push_str(&format!("-- {}\n", l));
    }
    s.push_str("BEGIN;\n");
    if let Some(g) = guard {
        s.push_str(g);
        s.push('\n');
    }
    for st in statements {
        s.push_str(st);
        s.push('\n');
    }
    s.push_str("COMMIT;\n");
    s
}

/// `yt-diff-<db>-<schema>.<table>-<node>-<ts>.sql`, each part fit for a file name
pub fn file_name(db: &str, schema: &str, table: &str, node: &str, ts: &str) -> String {
    let safe = |s: &str| s.chars().map(|c| if c.is_ascii_alphanumeric() || "-_.".contains(c) { c } else { '_' }).collect::<String>();
    format!("yt-diff-{}-{}.{}-{}-{}.sql", safe(db), safe(schema), safe(table), safe(node), safe(ts))
}

/// Unix seconds as `YYYYMMDDTHHMMSSZ`, UTC (the civil date from the day count, H. Hinnant's
/// days_from_civil inverted)
pub fn utc_stamp(secs: u64) -> String {
    let (days, rem) = ((secs / 86400) as i64, secs % 86400);
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    format!("{:04}{:02}{:02}T{:02}{:02}{:02}Z", y, m, d, rem / 3600, (rem % 3600) / 60, rem % 60)
}

/// A table over this many rows (on any node) is compared by key range
pub const LARGE_ROWS: i64 = 1_000_000;

/// A differing range of this many rows or fewer (on its larger side) is compared row by row; a bigger
/// one is halved first
pub const RANGE_ROWS: i64 = 20_000;

/// A range of the first key column, `[lo, hi)`. `hi` is wider than the key so the top range can end past
/// the largest key; the upper bound is left out of the SQL when it does not fit a key
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyRange {
    pub lo: i64,
    pub hi: i128,
}

/// The two halves of a range at its midpoint; None when it holds at most one key
pub fn halve(r: KeyRange) -> Option<(KeyRange, KeyRange)> {
    let width = r.hi - (r.lo as i128);
    if width < 2 {
        return None;
    }
    let mid = (r.lo as i128) + width / 2;
    Some((KeyRange { lo: r.lo, hi: mid }, KeyRange { lo: mid as i64, hi: r.hi }))
}

/// What one range's comparison decides
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// the node and the primary hold the same rows here
    Same,
    /// small enough, or one key wide: compared row by row
    Leaf,
    /// differs and is too large: its halves are compared next
    Split,
}

/// A range's verdict from its fold on the primary and on the node
pub fn step(r: KeyRange, primary: Fold, node: Fold, limit: i64) -> Step {
    if primary == node {
        return Step::Same;
    }
    let rows = primary.count.max(node.count);
    if rows <= limit || halve(r).is_none() { Step::Leaf } else { Step::Split }
}

/// `t.<col>` in `[lo, hi)`: the upper bound left out when it is past the largest key
pub fn range_sql(col: &str, r: KeyRange) -> String {
    let c = format!("t.{}", quote_ident(col));
    match i64::try_from(r.hi) {
        Ok(hi) => format!("{c} >= {} AND {c} < {}", r.lo, hi),
        Err(_) => format!("{c} >= {}", r.lo),
    }
}

/// A range's upper bound as a bigint literal (a negative one in parentheses, so the lowest bigint is in
/// range), NULL - no bound - past the largest key
pub fn bound_lit(hi: i128) -> String {
    match i64::try_from(hi) {
        Ok(h) if h < 0 => format!("({})::bigint", h),
        Ok(h) => format!("{}::bigint", h),
        Err(_) => "NULL::bigint".to_string(),
    }
}

/// A range's lower bound as a bigint literal: a negative one in parentheses, so the lowest bigint is in range
/// (`-9223372036854775808::bigint` casts before it negates, and is out of range - review 2026-11-11)
pub fn lo_lit(lo: i64) -> String {
    if lo < 0 { format!("({})::bigint", lo) } else { format!("{}::bigint", lo) }
}

/// Whether a leaf range is read in pages: one first key with a second key column to page by
pub fn pages_by_second_key(r: KeyRange, pk_len: usize) -> bool {
    halve(r).is_none() && pk_len > 1
}

/// Where a page's ids stop, so that every id up to it is complete on both sides: the smaller last id of
/// the pages that are full, None when neither is (the last page of both)
pub fn page_cut(p_full_last: Option<&str>, n_full_last: Option<&str>) -> Option<String> {
    match (p_full_last, n_full_last) {
        (Some(a), Some(b)) => Some(a.min(b).to_string()),
        (Some(a), None) => Some(a.to_string()),
        (None, Some(b)) => Some(b.to_string()),
        (None, None) => None,
    }
}

// ── the reads, on any client: a pool's connection derefs to one ─────────────────────────────────────

fn fold_of(row: &tokio_postgres::Row, at: usize) -> Fold {
    Fold { count: row.get(at), xor32: row.get::<_, i64>(at + 1) as u32, sum64: row.get(at + 2) }
}

/// The table's shape on the primary, with its rules
pub async fn shape_of(c: &Client, schema: &str, table: &str, rules: &TableRules) -> Result<Shape, String> {
    let rel = table_of(schema, table);
    let pk: Vec<String> = c.query(PK_SQL, &[&rel]).await.map_err(|e| pg_text(&e))?.iter().map(|r| r.get(0)).collect();
    let cols: Vec<Col> = c.query(COLS_SQL, &[&rel]).await.map_err(|e| pg_text(&e))?.iter()
        .map(|r| Col { name: r.get(0), generated: r.get(1), identity_always: r.get(2) }).collect();
    Shape::new(pk, cols, rules)
}

/// Phase 1: the table's fold on one node, read in the diff's session: its generated columns read first, and
/// left out as phase 2 leaves them out. A table without a primary key folds too (phase 2 refuses it)
pub async fn table_fold(c: &Client, schema: &str, table: &str, rules: &TableRules) -> Result<Fold, String> {
    let rel = table_of(schema, table);
    let generated: Vec<String> = c.query(GENERATED_SQL, &[&rel]).await.map_err(|e| pg_text(&e))?.iter().map(|r| r.get(0)).collect();
    c.batch_execute(SESSION_SQL).await.map_err(|e| pg_text(&e))?;
    let row = c.query_one(table_fold_sql(schema, table, rules, &generated).as_str(), &[]).await.map_err(|e| pg_text(&e))?;
    Ok(fold_of(&row, 0))
}

/// Each row's id and hash in one range (None: the whole table)
pub async fn key_md5s_range(c: &Client, schema: &str, table: &str, shape: &Shape, range: Option<KeyRange>)
    -> Result<BTreeMap<String, String>, String>
{
    c.batch_execute(SESSION_SQL).await.map_err(|e| pg_text(&e))?;
    let rows = c.query(shape.key_md5_range_sql(schema, table, range).as_str(), &[]).await.map_err(|e| pg_text(&e))?;
    ids_of(rows.iter().map(|r| (r.get(0), r.get(1))))
}

/// Each row's id and hash, the whole table
pub async fn key_md5s(c: &Client, schema: &str, table: &str, shape: &Shape) -> Result<BTreeMap<String, String>, String> {
    key_md5s_range(c, schema, table, shape, None).await
}

/// One page of a range's ids and hashes
pub async fn key_md5s_page(c: &Client, schema: &str, table: &str, shape: &Shape, range: KeyRange, after: Option<&str>, limit: usize)
    -> Result<BTreeMap<String, String>, String>
{
    c.batch_execute(SESSION_SQL).await.map_err(|e| pg_text(&e))?;
    let rows = c.query(shape.key_md5_page_sql(schema, table, range, after, limit).as_str(), &[]).await.map_err(|e| pg_text(&e))?;
    ids_of(rows.iter().map(|r| (r.get(0), r.get(1))))
}

/// The whole rows of these ids, in pages of 500
pub async fn rows_of(c: &Client, schema: &str, table: &str, shape: &Shape, ids: &[String]) -> Result<Vec<(String, String)>, String> {
    c.batch_execute(SESSION_SQL).await.map_err(|e| pg_text(&e))?;
    let sql = shape.rows_sql(schema, table);
    let mut out = Vec::new();
    for page in ids.chunks(500) {
        let page: Vec<String> = page.to_vec();
        out.extend(c.query(sql.as_str(), &[&page]).await.map_err(|e| pg_text(&e))?.iter()
            .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1))));
    }
    Ok(out)
}

/// A repair file's SQL run on the node: one transaction; on an error, rolled back on that connection
pub async fn apply(c: &Client, sql: &str) -> Result<(), String> {
    match c.batch_execute(sql).await {
        Ok(()) => Ok(()),
        Err(e) => {
            let _ = c.batch_execute("ROLLBACK").await;
            Err(pg_text(&e))
        }
    }
}

/// Whether the first key column is an integer (smallint, integer, bigint): ranges are cut on one only
pub async fn first_key_is_integer(c: &Client, schema: &str, table: &str, shape: &Shape) -> Result<bool, String> {
    let rel = table_of(schema, table);
    let key = shape.range_col();
    let row = c.query_opt(KEY_TYPE_SQL, &[&rel, &key]).await.map_err(|e| pg_text(&e))?;
    Ok(row.is_some_and(|r| matches!(r.get::<_, String>(0).as_str(), "smallint" | "integer" | "bigint")))
}

/// The smallest and largest first key of the rows (the filter's); None when there are none
pub async fn key_bounds(c: &Client, schema: &str, table: &str, shape: &Shape) -> Result<Option<(i64, i64)>, String> {
    c.batch_execute(SESSION_SQL).await.map_err(|e| pg_text(&e))?;
    let col = format!("t.{}", quote_ident(shape.range_col()));
    let sql = format!("SELECT min({col})::bigint, max({col})::bigint FROM {} t{}", table_of(schema, table), shape.where_clause(None));
    let row = c.query_one(sql.as_str(), &[]).await.map_err(|e| pg_text(&e))?;
    Ok(row.get::<_, Option<i64>>(0).zip(row.get::<_, Option<i64>>(1)))
}

/// Each range's fold on one node, in the ranges' order
pub async fn range_folds(c: &Client, schema: &str, table: &str, shape: &Shape, ranges: &[KeyRange]) -> Result<Vec<Fold>, String> {
    let mut out = vec![Fold::default(); ranges.len()];
    if ranges.is_empty() {
        return Ok(out);
    }
    c.batch_execute(SESSION_SQL).await.map_err(|e| pg_text(&e))?;
    for r in c.query(shape.range_fold_sql(schema, table, ranges).as_str(), &[]).await.map_err(|e| pg_text(&e))? {
        let i: i32 = r.get(0);
        out[i as usize] = fold_of(&r, 1);
    }
    Ok(out)
}

/// The repair plan of one node for a table over LARGE_ROWS: the key span of both sides is folded per range
/// (one statement per side), a range that differs and is too large is halved ([`step`]), and only the small
/// differing ranges are read row by row; their plans, merged, are the table's
pub async fn ranged_plan(p: &Client, n: &Client, schema: &str, table: &str, shape: &Shape, limit: i64) -> Result<Plan, String> {
    let mut found: Vec<(i64, i64)> = Vec::new();
    for c in [p, n] {
        if let Some(b) = key_bounds(c, schema, table, shape).await? {
            found.push(b);
        }
    }
    let (Some(lo), Some(hi)) = (found.iter().map(|b| b.0).min(), found.iter().map(|b| b.1).max()) else {
        return Ok(Plan::default());
    };
    let mut todo = vec![KeyRange { lo, hi: hi as i128 + 1 }];
    let mut leaves: Vec<KeyRange> = Vec::new();
    while !todo.is_empty() {
        let pf = range_folds(p, schema, table, shape, &todo).await?;
        let nf = range_folds(n, schema, table, shape, &todo).await?;
        let mut next = Vec::new();
        for ((r, a), b) in todo.iter().zip(pf).zip(nf) {
            match step(*r, a, b, limit) {
                Step::Same => {}
                Step::Leaf => leaves.push(*r),
                Step::Split => {
                    if let Some((x, y)) = halve(*r) {
                        next.push(x);
                        next.push(y);
                    }
                }
            }
        }
        todo = next;
    }
    let mut plans = Vec::with_capacity(leaves.len());
    for r in leaves {
        plans.push(leaf_plan(p, n, schema, table, shape, r, limit).await?);
    }
    Ok(Plan::merge(plans))
}

/// One leaf range's plan: a one-key range with a second key column is read a page of `page` ids at a
/// time ([`page_cut`] says how far a page is complete); any other leaf is read whole
async fn leaf_plan(p: &Client, n: &Client, schema: &str, table: &str, shape: &Shape, r: KeyRange, page: i64) -> Result<Plan, String> {
    if !pages_by_second_key(r, shape.pk.len()) {
        let pm = key_md5s_range(p, schema, table, shape, Some(r)).await?;
        let nm = key_md5s_range(n, schema, table, shape, Some(r)).await?;
        return Ok(Plan::of(&pm, &nm));
    }
    let page = page.max(1) as usize;
    let mut plans = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let pm = key_md5s_page(p, schema, table, shape, r, after.as_deref(), page).await?;
        let nm = key_md5s_page(n, schema, table, shape, r, after.as_deref(), page).await?;
        let full_last = |m: &BTreeMap<String, String>| if m.len() >= page { m.keys().next_back().cloned() } else { None };
        let Some(cut) = page_cut(full_last(&pm).as_deref(), full_last(&nm).as_deref()) else {
            plans.push(Plan::of(&pm, &nm));
            break;
        };
        let p_part: BTreeMap<String, String> = pm.into_iter().filter(|(k, _)| *k <= cut).collect();
        let n_part: BTreeMap<String, String> = nm.into_iter().filter(|(k, _)| *k <= cut).collect();
        plans.push(Plan::of(&p_part, &n_part));
        after = Some(cut);
    }
    Ok(Plan::merge(plans))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ytctl's tests of these (tui/data_diff.rs), on the first-class side's id, basis and fold

    fn shape() -> Shape {
        let rules = TableRules::new(Some("seen_at, node"), Some("tenant = 1"));
        Shape::new(vec!["id".into()], vec![
            Col { name: "id".into(), generated: false, identity_always: true },
            Col { name: "item".into(), generated: false, identity_always: false },
            Col { name: "seen_at".into(), generated: false, identity_always: false },
            Col { name: "node".into(), generated: false, identity_always: false },
            Col { name: "total".into(), generated: true, identity_always: false },
            Col { name: "it's".into(), generated: false, identity_always: false },
        ], &rules).unwrap()
    }

    /// a node's rows are compared by their id, over every column but the ignored and generated ones,
    /// the filter's rows only
    #[test]
    fn rows_are_compared_by_id_over_what_replicates() {
        let s = shape();
        assert_eq!(s.ignored, vec!["seen_at", "node"]);
        assert_eq!(s.key_md5_sql("public", "orders"), "SELECT (t.\"id\"::text), md5((to_jsonb(t.*) - \
            ARRAY['seen_at','node','total']::text[])::text) FROM \"public\".\"orders\" t WHERE (tenant = 1)");
        assert!(s.rows_sql("public", "orders").ends_with("WHERE (t.\"id\"::text) = ANY ($1::text[])"));
        let plain = Shape::new(vec!["a".into(), "b".into()], vec![], &TableRules::default()).unwrap();
        assert_eq!(plain.key_md5_sql("s", "t"),
            "SELECT concat_ws('|', t.\"a\"::text, t.\"b\"::text), md5(to_jsonb(t.*)::text) FROM \"s\".\"t\" t");
        assert!(Shape::new(vec![], vec![], &TableRules::default()).unwrap_err().contains("no primary key"));
        assert!(Shape::new(vec!["id".into()], vec![], &TableRules::new(Some("id"), None)).unwrap_err().contains("in the key"));
    }

    /// the id is ytserv's: the same text as recon's pk_expr for plain names, quoted for any other
    #[test]
    fn the_id_and_the_basis_are_ytservs() {
        let s = Shape::new(vec!["a".into(), "b".into()], vec![], &TableRules::default()).unwrap();
        assert_eq!(s.id_expr().replace('"', ""), super::super::recon::pk_expr(&["a".into(), "b".into()]));
        assert_eq!(basis(&["note".into()]), super::super::recon::row_basis(&["note".into()]));
        assert_eq!(basis(&[]), super::super::recon::row_basis(&[]));
        assert_eq!(basis(&["it's".into()]), "(to_jsonb(t.*) - ARRAY['it''s']::text[])::text", "any name, quoted");
    }

    /// two rows of one id refuse the table: a repair by an ambiguous id would be a guess
    #[test]
    fn two_rows_of_one_id_refuse_the_table() {
        let ok = ids_of(vec![("1".to_string(), "a".to_string()), ("2".to_string(), "b".to_string())]).unwrap();
        assert_eq!(ok.len(), 2);
        let why = ids_of(vec![("a|b|c".to_string(), "x".to_string()), ("a|b|c".to_string(), "y".to_string())]).unwrap_err();
        assert!(why.contains("[a|b|c]") && why.contains("'|'"), "{}", why);
    }

    /// the repair: an upsert writes every column but the generated, sets the compared ones, overrides an
    /// identity; a delete goes by the key columns of the node's row
    #[test]
    fn the_repair_upserts_and_deletes_by_key() {
        let s = shape();
        let u = s.upsert_sql("public", "orders", r#"{"id": 7, "item": "it's", "total": 3}"#);
        assert_eq!(u, "INSERT INTO \"public\".\"orders\" (\"id\", \"item\", \"seen_at\", \"node\", \"it's\") OVERRIDING SYSTEM VALUE \
            SELECT \"id\", \"item\", \"seen_at\", \"node\", \"it's\" FROM jsonb_populate_record(NULL::\"public\".\"orders\", \
            '{\"id\": 7, \"item\": \"it''s\", \"total\": 3}'::jsonb) ON CONFLICT (\"id\") DO UPDATE SET \"item\" = EXCLUDED.\"item\", \
            \"it's\" = EXCLUDED.\"it's\";");
        assert_eq!(s.delete_sql("public", "orders", r#"{"id": 9, "item": "x"}"#), "DELETE FROM \"public\".\"orders\" WHERE (\"id\") IN \
            (SELECT \"id\" FROM jsonb_populate_record(NULL::\"public\".\"orders\", '{\"id\": 9, \"item\": \"x\"}'::jsonb));");
        let key_only = Shape::new(vec!["id".into()], vec![Col { name: "id".into(), generated: false, identity_always: false }],
            &TableRules::default()).unwrap();
        assert!(key_only.upsert_sql("s", "t", "{}").ends_with("ON CONFLICT (\"id\") DO NOTHING;"));
        let sql = repair_sql("from yt1\n2 upsert(s)", Some(YT_LOCAL_GUARD), &["SELECT 1;".to_string()]);
        assert_eq!(sql, "-- from yt1\n-- 2 upsert(s)\nBEGIN;\nSET LOCAL yt.node = 'local';\nSELECT 1;\nCOMMIT;\n");
        assert_eq!(repair_sql("", None, &[]), "BEGIN;\nCOMMIT;\n");
    }

    #[test]
    fn the_plan_is_what_differs() {
        let m = |v: &[(&str, &str)]| v.iter().map(|(k, h)| (k.to_string(), h.to_string())).collect::<BTreeMap<_, _>>();
        let p = Plan::of(&m(&[("1", "a"), ("2", "b"), ("3", "c")]), &m(&[("1", "a"), ("2", "x"), ("4", "d")]));
        assert_eq!(p, Plan { upsert: vec!["2".into(), "3".into()], delete: vec!["4".into()] });
        assert!(Plan::of(&m(&[("1", "a")]), &m(&[("1", "a")])).is_empty());
        assert_eq!(file_name("app", "public", "orders", "yt 2/x", "20261022T171500Z"), "yt-diff-app-public.orders-yt_2_x-20261022T171500Z.sql");
    }

    /// phase 1 folds what phase 2 compares, as ytserv folds a frame: the filter's rows, ignore_col out
    #[test]
    fn the_table_fold_is_over_what_replicates() {
        let q = table_fold_sql("public", "orders", &TableRules::new(Some("seen_at"), Some("tenant = 1")), &[]);
        assert!(q.starts_with("WITH b AS (SELECT ('x' || md5((to_jsonb(t.*) - ARRAY['seen_at']::text[])::text))::bit(32)::int AS c32, \
            ('x' || substr(md5((to_jsonb(t.*) - ARRAY['seen_at']::text[])::text), 1, 15))::bit(60)::bigint AS c60 \
            FROM \"public\".\"orders\" t WHERE (tenant = 1)) SELECT count(*)::bigint, coalesce(bit_xor(c32), 0)::bigint, "), "{}", q);
        assert!(q.ends_with(&format!("{} FROM b", SUM64_WRAPPED)), "{}", q);
        assert!(table_fold_sql("s", "t", &TableRules::default(), &[]).contains("md5(to_jsonb(t.*)::text)"));
        // the generated columns are left out as phase 2 leaves them out
        let g = table_fold_sql("s", "t", &TableRules::new(Some("seen_at"), None), &["total".to_string()]);
        assert!(g.contains("ARRAY['seen_at','total']"), "{}", g);
        assert!(shape().key_md5_sql("s", "t").contains("ARRAY['seen_at','node','total']"), "phase 2 the same: ignored, then generated");
    }

    /// a fold's signature is one value of both folds: equal folds, equal signatures
    #[test]
    fn a_folds_signature_keeps_both_folds() {
        let a = Fold { count: 3, xor32: 0xdead_beef, sum64: 12345 };
        assert_eq!(a.signature(), a.signature());
        assert_ne!(a.signature(), Fold { xor32: 0xdead_beee, ..a }.signature());
        assert_ne!(a.signature(), Fold { sum64: 12346, ..a }.signature());
        assert_eq!(Fold::default().signature(), 0);
    }

    #[test]
    fn a_stamp_is_the_utc_date_and_time() {
        assert_eq!(utc_stamp(0), "19700101T000000Z");
        assert_eq!(utc_stamp(951782400), "20000229T000000Z");
        assert_eq!(utc_stamp(1792689089), "20261022T171129Z");
        assert_eq!(utc_stamp(4102444799), "20991231T235959Z");
    }

    #[test]
    fn a_key_range_is_halved_at_its_midpoint() {
        let r = |lo: i64, hi: i128| KeyRange { lo, hi };
        assert_eq!(halve(r(0, 10)), Some((r(0, 5), r(5, 10))));
        assert_eq!(halve(r(0, 3)), Some((r(0, 1), r(1, 3))));
        assert_eq!(halve(r(0, 2)), Some((r(0, 1), r(1, 2))));
        assert_eq!(halve(r(7, 8)), None, "one key is not halved");
        assert_eq!(halve(r(7, 7)), None, "an empty range is not halved");
        let top = r(i64::MAX - 1, i64::MAX as i128 + 1);
        assert_eq!(halve(top), Some((r(i64::MAX - 1, i64::MAX as i128), r(i64::MAX, i64::MAX as i128 + 1))));
    }

    #[test]
    fn a_range_is_split_only_while_it_differs_and_is_large() {
        let f = |count: i64, x: u32| Fold { count, xor32: x, sum64: x as i64 };
        let wide = KeyRange { lo: 0, hi: 10 };
        assert_eq!(step(wide, f(5, 1), f(5, 1), 3), Step::Same, "equal folds are the same rows");
        assert_eq!(step(wide, f(2, 1), f(3, 9), 3), Step::Leaf, "three rows or fewer are read row by row");
        assert_eq!(step(wide, f(4, 1), f(3, 9), 3), Step::Split, "a bigger differing range is halved");
        assert_eq!(step(KeyRange { lo: 4, hi: 5 }, f(4, 1), f(3, 9), 3), Step::Leaf, "one key is read row by row");
        assert_eq!(step(wide, Fold { count: 5, xor32: 1, sum64: 1 }, Fold { count: 5, xor32: 1, sum64: 2 }, 3), Step::Split,
            "the sum tells apart what the xor does not");
    }

    #[test]
    fn a_range_is_a_predicate_on_its_first_key() {
        assert_eq!(range_sql("id", KeyRange { lo: 0, hi: 5 }), "t.\"id\" >= 0 AND t.\"id\" < 5");
        assert_eq!(range_sql("id", KeyRange { lo: 7, hi: i64::MAX as i128 + 1 }), "t.\"id\" >= 7");
        assert_eq!(bound_lit(5), "5::bigint");
        assert_eq!(bound_lit(i64::MAX as i128 + 1), "NULL::bigint");
        assert_eq!(bound_lit(i64::MIN as i128), "(-9223372036854775808)::bigint");
        assert_eq!(bound_lit(-5), "(-5)::bigint");
    }

    #[test]
    fn the_key_query_reads_only_the_range_and_the_filter() {
        let s = shape();
        let q = s.key_md5_range_sql("public", "orders", Some(KeyRange { lo: 0, hi: 5 }));
        assert!(q.ends_with("FROM \"public\".\"orders\" t WHERE (tenant = 1) AND (t.\"id\" >= 0 AND t.\"id\" < 5)"), "{}", q);
        assert_eq!(s.key_md5_range_sql("public", "orders", None), s.key_md5_sql("public", "orders"));
    }

    /// the ranges are folded in one statement over a VALUES list, an empty range folding to none
    #[test]
    fn the_range_folds_are_one_statement_over_a_values_list() {
        let s = shape();
        let q = s.range_fold_sql("public", "orders", &[KeyRange { lo: 0, hi: 5 }, KeyRange { lo: 5, hi: i64::MAX as i128 + 1 }]);
        assert!(q.contains("(VALUES (0, 0::bigint, 5::bigint), (1, 5::bigint, NULL::bigint)) AS r(i, lo, hi)"), "{}", q);
        assert!(q.contains("LEFT JOIN \"public\".\"orders\" t ON t.\"id\" >= r.lo AND (r.hi IS NULL OR t.\"id\" < r.hi) AND (tenant = 1)"), "{}", q);
        assert!(q.contains("SELECT i, count(k)::bigint, coalesce(bit_xor(c32), 0)::bigint, "), "{}", q);
        assert!(q.ends_with("FROM b GROUP BY i ORDER BY i"), "{}", q);
        // the lowest bigint as a range's start: parenthesised, so it is in range
        let low = s.range_fold_sql("public", "orders", &[KeyRange { lo: i64::MIN, hi: 0 }]);
        assert!(low.contains("(VALUES (0, (-9223372036854775808)::bigint, 0::bigint))"), "{}", low);
        assert_eq!(lo_lit(-5), "(-5)::bigint");
        assert_eq!(lo_lit(5), "5::bigint");
    }

    #[test]
    fn the_plans_of_disjoint_ranges_add_up() {
        let a = Plan { upsert: vec!["1".into()], delete: vec![] };
        let b = Plan { upsert: vec!["5".into()], delete: vec!["9".into()] };
        assert_eq!(Plan::merge(vec![a, b]), Plan { upsert: vec!["1".into(), "5".into()], delete: vec!["9".into()] });
        assert!(Plan::merge(vec![]).is_empty());
    }

    #[test]
    fn a_one_key_page_takes_the_ids_after_its_last_in_byte_order_up_to_its_limit() {
        let sql = shape().key_md5_page_sql("public", "t", KeyRange { lo: 5, hi: 6 }, Some("abc"), 20000);
        assert!(sql.contains("(t.\"id\"::text) COLLATE \"C\" > 'abc'"), "{}", sql);
        assert!(sql.contains("t.\"id\" >= 5 AND t.\"id\" < 6"), "the one key: {}", sql);
        assert!(sql.ends_with("ORDER BY (t.\"id\"::text) COLLATE \"C\" LIMIT 20000"), "{}", sql);
        let first = shape().key_md5_page_sql("public", "t", KeyRange { lo: 5, hi: 6 }, None, 7);
        assert!(!first.contains("> '"), "the first page has no after: {}", first);
    }

    #[test]
    fn a_one_key_leaf_is_paged_by_its_second_key_and_a_page_stops_at_the_smaller_full_last_id() {
        assert!(pages_by_second_key(KeyRange { lo: 5, hi: 6 }, 2), "one key, a second column");
        assert!(!pages_by_second_key(KeyRange { lo: 5, hi: 6 }, 1), "one key, no second column: read whole");
        assert!(!pages_by_second_key(KeyRange { lo: 5, hi: 9 }, 2), "several keys: read whole");
        assert_eq!(page_cut(Some("b"), Some("a")), Some("a".to_string()));
        assert_eq!(page_cut(Some("b"), None), Some("b".to_string()), "a short node page is complete");
        assert_eq!(page_cut(None, Some("c")), Some("c".to_string()), "a short primary page is complete");
        assert_eq!(page_cut(None, None), None, "the last page of both");
    }

    #[test]
    fn the_rules_read_the_column_list_and_the_filter() {
        let r = TableRules::new(Some(" a, ,b "), Some("  "));
        assert_eq!((r.ignore, r.filter), (vec!["a".to_string(), "b".to_string()], None));
        assert_eq!(TableRules::new(None, Some(" x > 1 ")).filter.as_deref(), Some("x > 1"));
    }
}

use std::collections::{HashMap, HashSet};
use std::io::Write;

use chrono::Duration;
use tokio_postgres::Client;
use tracing::debug;

use crate::config::{parse_table_name, ProgressEvent};
use crate::error::{DataDiffError, Result};
use crate::metadata::{get_columns, resolve_pk_columns, table_exists};
use crate::sql_gen::{
    build_check_query, format_table_name, generate_insert, generate_update, pk_text_list, quote_identifier,
    select_list,
};
use crate::types::{extract_row, text_key, ChunkStrategy, ColumnInfo, DiffStats, Row, TableAnalysis, TableDiffStatus};

/// Compares data between a reference and target table,
/// generating INSERT statements for missing rows and UPDATE statements for modified rows.
pub struct TableDataDiff<'a> {
    ref_client: &'a Client,
    target_client: &'a Client,
    ref_table: String,
    target_table: String,
    chunk_size: usize,
    columns_to_ignore: HashSet<String>,
    exclude_columns: HashSet<String>,
    alternate_keys: Option<Vec<String>>,
    exclude_real_pk: bool,
    /// Additional WHERE condition for the reference table query.
    table_where: Option<String>,
    /// Whether to include identity/auto-increment columns in generated SQL.
    include_identity_columns: Option<bool>,
    /// Analysis result from the estimation phase (determines chunk strategy).
    analysis: Option<TableAnalysis>,
    /// Progress callback.
    on_progress: Option<std::sync::Arc<dyn Fn(ProgressEvent) + Send + Sync>>,
    cancelled: bool,
}

impl<'a> TableDataDiff<'a> {
    pub fn new(
        ref_client: &'a Client,
        target_client: &'a Client,
        ref_table: String,
        target_table: String,
    ) -> Self {
        Self {
            ref_client,
            target_client,
            ref_table,
            target_table,
            chunk_size: 25,
            columns_to_ignore: HashSet::new(),
            exclude_columns: HashSet::new(),
            alternate_keys: None,
            exclude_real_pk: false,
            table_where: None,
            include_identity_columns: None,
            analysis: None,
            on_progress: None,
            cancelled: false,
        }
    }

    pub fn set_chunk_size(&mut self, size: usize) {
        self.chunk_size = size.max(1);
    }

    pub fn set_columns_to_ignore(&mut self, cols: HashSet<String>) {
        self.columns_to_ignore = cols.into_iter().map(|c| c.to_lowercase()).collect();
    }

    pub fn set_exclude_columns(&mut self, cols: HashSet<String>) {
        self.exclude_columns = cols.into_iter().map(|c| c.to_lowercase()).collect();
    }

    pub fn set_alternate_keys(&mut self, keys: Option<Vec<String>>) {
        self.alternate_keys = keys;
    }

    pub fn set_exclude_real_pk(&mut self, flag: bool) {
        self.exclude_real_pk = flag;
    }

    pub fn set_table_where(&mut self, condition: Option<String>) {
        self.table_where = condition.map(|w| {
            let trimmed = w.trim().to_string();
            // Strip leading WHERE keyword if present
            if trimmed.len() >= 5 && trimmed[..5].eq_ignore_ascii_case("WHERE") {
                trimmed[5..].trim().to_string()
            } else {
                trimmed
            }
        });
    }

    pub fn set_include_identity_columns(&mut self, flag: Option<bool>) {
        self.include_identity_columns = flag;
    }

    pub fn set_analysis(&mut self, analysis: Option<TableAnalysis>) {
        self.analysis = analysis;
    }

    pub fn set_on_progress(
        &mut self,
        cb: Option<std::sync::Arc<dyn Fn(ProgressEvent) + Send + Sync>>,
    ) {
        self.on_progress = cb;
    }

    pub fn cancel(&mut self) {
        self.cancelled = true;
    }

    /// Validate that both tables exist and have compatible columns.
    /// Returns (ref_columns, target_columns, pk_columns, real_pk_names).
    ///
    /// Target must exist. All reference columns must exist in target (target may have extra columns).
    async fn setup(
        &self,
    ) -> std::result::Result<
        (Vec<ColumnInfo>, Vec<ColumnInfo>, Vec<ColumnInfo>, Vec<String>),
        TableDiffStatus,
    > {
        // Check reference table
        let ref_exists = table_exists(self.ref_client, &self.ref_table)
            .await
            .unwrap_or(false);
        if !ref_exists {
            return Err(TableDiffStatus::ReferenceNotFound);
        }

        let ref_columns = get_columns(self.ref_client, &self.ref_table)
            .await
            .map_err(|_| TableDiffStatus::ReferenceNotFound)?;

        // Check target table
        let target_exists = table_exists(self.target_client, &self.target_table)
            .await
            .unwrap_or(false);

        if !target_exists {
            return Err(TableDiffStatus::TargetNotFound);
        }

        let target_columns = get_columns(self.target_client, &self.target_table)
            .await
            .map_err(|_| TableDiffStatus::TargetNotFound)?;

        // Verify all reference columns exist in target (target may be a superset)
        for rc in &ref_columns {
            if !target_columns
                .iter()
                .any(|tc| tc.name.eq_ignore_ascii_case(&rc.name))
            {
                return Err(TableDiffStatus::ColumnMismatch);
            }
        }

        // Resolve PK columns
        let (pk_cols, real_pk_names) =
            resolve_pk_columns(&ref_columns, self.alternate_keys.as_deref());

        if pk_cols.is_empty() {
            return Err(TableDiffStatus::NoPrimaryKey);
        }

        Ok((ref_columns, target_columns, pk_cols, real_pk_names))
    }

    /// Execute the diff: compare reference with target, writing INSERT/UPDATE SQL.
    ///
    /// Dispatches to `execute_md5_timestamp()` if the analysis strategy is `Md5TimestampChunk`,
    /// otherwise uses the default row-by-row comparison.
    pub async fn execute(
        &mut self,
        insert_writer: &mut impl Write,
        update_writer: &mut impl Write,
    ) -> Result<DiffStats> {
        // Check if we should use the MD5 timestamp path
        let use_md5 = matches!(
            self.analysis.as_ref().map(|a| &a.strategy),
            Some(ChunkStrategy::Md5TimestampChunk { .. })
        );

        if use_md5 {
            return self
                .execute_md5_timestamp(insert_writer, update_writer)
                .await;
        }

        self.execute_row_by_row(insert_writer, update_writer).await
    }

    /// Standard row-by-row comparison (for small/medium tables or large excluded tables).
    async fn execute_row_by_row(
        &mut self,
        insert_writer: &mut impl Write,
        update_writer: &mut impl Write,
    ) -> Result<DiffStats> {
        let (ref_columns, target_columns, pk_cols, real_pk_names) =
            self.setup().await.map_err(|status| match status {
                TableDiffStatus::ReferenceNotFound => {
                    DataDiffError::TableNotFound(self.ref_table.clone())
                }
                TableDiffStatus::TargetNotFound => {
                    DataDiffError::TableNotFound(self.target_table.clone())
                }
                TableDiffStatus::NoPrimaryKey => {
                    DataDiffError::NoPrimaryKey(self.ref_table.clone())
                }
                TableDiffStatus::ColumnMismatch => DataDiffError::ColumnMismatch {
                    ref_table: self.ref_table.clone(),
                    target_table: self.target_table.clone(),
                },
                TableDiffStatus::Ok => unreachable!(),
            })?;

        let exclude_from_output = self.build_exclude_set(&ref_columns, &real_pk_names);

        let mut stats = DiffStats::default();
        let mut first_insert = true;
        let mut first_update = true;

        // Build the reference table SELECT query
        let ref_table_sql = format_table_name(&self.ref_table);
        let mut retrieve_sql = format!("SELECT {} FROM {}", select_list(&ref_columns), ref_table_sql);

        if let Some(ref where_cond) = self.table_where {
            retrieve_sql.push_str(" WHERE ");
            retrieve_sql.push_str(where_cond);
        }

        debug!("Retrieving reference rows: {}", retrieve_sql);
        let ref_rows = self.ref_client.query(&retrieve_sql, &[]).await?;

        let mut chunk: Vec<Row> = Vec::with_capacity(self.chunk_size);

        for pg_row in &ref_rows {
            if self.cancelled {
                return Err(DataDiffError::Cancelled);
            }

            let row = extract_row(pg_row, &ref_columns);
            chunk.push(row);

            if chunk.len() == self.chunk_size {
                self.check_rows(
                    &chunk,
                    &ref_columns,
                    &target_columns,
                    &pk_cols,
                    &exclude_from_output,
                    insert_writer,
                    update_writer,
                    &mut stats,
                    &mut first_insert,
                    &mut first_update,
                )
                .await?;
                chunk.clear();
            }
        }

        // Process remaining rows
        if !chunk.is_empty() && !self.cancelled {
            self.check_rows(
                &chunk,
                &ref_columns,
                &target_columns,
                &pk_cols,
                &exclude_from_output,
                insert_writer,
                update_writer,
                &mut stats,
                &mut first_insert,
                &mut first_update,
            )
            .await?;
        }

        Ok(stats)
    }

    /// MD5 timestamp-based comparison for large tables with a timestamp column.
    ///
    /// Splits the timestamp range into chunks, computes MD5 hashes of each row in the chunk,
    /// and only fetches full rows where hashes differ or rows are missing.
    async fn execute_md5_timestamp(
        &mut self,
        insert_writer: &mut impl Write,
        update_writer: &mut impl Write,
    ) -> Result<DiffStats> {
        let (ref_columns, target_columns, pk_cols, real_pk_names) =
            self.setup().await.map_err(|status| match status {
                TableDiffStatus::ReferenceNotFound => {
                    DataDiffError::TableNotFound(self.ref_table.clone())
                }
                TableDiffStatus::TargetNotFound => {
                    DataDiffError::TableNotFound(self.target_table.clone())
                }
                TableDiffStatus::NoPrimaryKey => {
                    DataDiffError::NoPrimaryKey(self.ref_table.clone())
                }
                TableDiffStatus::ColumnMismatch => DataDiffError::ColumnMismatch {
                    ref_table: self.ref_table.clone(),
                    target_table: self.target_table.clone(),
                },
                TableDiffStatus::Ok => unreachable!(),
            })?;

        let exclude_from_output = self.build_exclude_set(&ref_columns, &real_pk_names);

        // Extract timestamp chunk parameters from the analysis
        let (ts_col, min_ts, max_ts, chunk_days) = match self.analysis.as_ref() {
            Some(TableAnalysis {
                strategy:
                    ChunkStrategy::Md5TimestampChunk {
                        timestamp_column,
                        min_ts,
                        max_ts,
                        chunk_days,
                    },
                ..
            }) => (timestamp_column.clone(), *min_ts, *max_ts, *chunk_days),
            _ => unreachable!("execute_md5_timestamp called without Md5TimestampChunk strategy"),
        };

        // Build the shared column list (only ref columns that exist in target)
        let shared_columns: Vec<&ColumnInfo> = ref_columns
            .iter()
            .filter(|rc| {
                target_columns
                    .iter()
                    .any(|tc| tc.name.eq_ignore_ascii_case(&rc.name))
            })
            .collect();

        let col_list: String = shared_columns
            .iter()
            .map(|c| quote_identifier(&c.name))
            .collect::<Vec<_>>()
            .join(", ");

        let pk_col_list: String = pk_cols
            .iter()
            .map(|c| quote_identifier(&c.name))
            .collect::<Vec<_>>()
            .join(", ");
        // a row is matched across the two sides by its key as text, whatever the key's types
        let pk_text = pk_text_list(&pk_cols);
        let ref_columns_shared: Vec<ColumnInfo> = shared_columns.iter().map(|c| (*c).clone()).collect();
        let shared_select = select_list(&ref_columns_shared);

        // Calculate number of chunks
        let total_duration = max_ts.signed_duration_since(min_ts);
        let total_days = total_duration.num_seconds() as f64 / 86400.0;
        let num_chunks = if total_days > 0.0 {
            (total_days / chunk_days).ceil() as usize
        } else {
            1
        };

        let mut stats = DiffStats::default();
        let mut first_insert = true;
        let mut first_update = true;

        let ref_table_sql = format_table_name(&self.ref_table);
        let target_table_sql = format_table_name(&self.target_table);
        let ts_col_sql = quote_identifier(&ts_col);
        // the filter takes the reference's rows; a target row is looked at by a reference row's key only
        let ref_filter = self.table_where.as_ref().map(|w| format!(" AND ({})", w)).unwrap_or_default();

        for chunk_idx in 0..num_chunks {
            if self.cancelled {
                return Err(DataDiffError::Cancelled);
            }

            let chunk_start = min_ts + Duration::seconds((chunk_idx as f64 * chunk_days * 86400.0) as i64);
            let chunk_end = if chunk_idx == num_chunks - 1 {
                // Last chunk: include the max timestamp
                max_ts + Duration::seconds(1)
            } else {
                min_ts + Duration::seconds(((chunk_idx + 1) as f64 * chunk_days * 86400.0) as i64)
            };

            // Emit progress
            if let Some(ref cb) = self.on_progress {
                cb(ProgressEvent::ChunkProgress {
                    table: self.ref_table.clone(),
                    chunk_index: chunk_idx,
                    total_chunks: num_chunks,
                });
            }

            // Query MD5 hashes from reference
            let ref_md5_sql = format!(
                "SELECT md5(ROW({col_list})::text), {pk_text} FROM {table} \
                 WHERE {ts} >= $1 AND {ts} < $2{filter} ORDER BY {pk_cols}",
                col_list = col_list,
                pk_text = pk_text,
                pk_cols = pk_col_list,
                table = ref_table_sql,
                ts = ts_col_sql,
                filter = ref_filter,
            );

            let ref_md5_rows = self
                .ref_client
                .query(&ref_md5_sql, &[&chunk_start, &chunk_end])
                .await?;

            // Query MD5 hashes from target
            let target_md5_sql = format!(
                "SELECT md5(ROW({col_list})::text), {pk_text} FROM {table} \
                 WHERE {ts} >= $1 AND {ts} < $2 ORDER BY {pk_cols}",
                col_list = col_list,
                pk_text = pk_text,
                pk_cols = pk_col_list,
                table = target_table_sql,
                ts = ts_col_sql,
            );

            let target_md5_rows = self
                .target_client
                .query(&target_md5_sql, &[&chunk_start, &chunk_end])
                .await?;

            // Build hash maps: pk_values_string -> md5_hash
            let ref_hashes = build_md5_map(&ref_md5_rows, pk_cols.len());
            let target_hashes = build_md5_map(&target_md5_rows, pk_cols.len());

            // Find PKs where hash differs or missing in target
            let diff_pks: HashSet<&String> = ref_hashes
                .iter()
                .filter(|(pk_key, ref_hash)| target_hashes.get(*pk_key) != Some(*ref_hash))
                .map(|(pk_key, _)| pk_key)
                .collect();

            if diff_pks.is_empty() {
                continue;
            }

            // Fetch full rows for the differing PKs and run check_rows
            // We need to build WHERE clauses from the PK values
            // Re-query full rows from reference for this chunk with timestamp filter
            let ref_full_sql = format!(
                "SELECT {cols}, {pk_text} FROM {table} WHERE {ts} >= $1 AND {ts} < $2{filter} ORDER BY {pk_cols}",
                filter = ref_filter,
                cols = shared_select,
                pk_text = pk_text,
                table = ref_table_sql,
                ts = ts_col_sql,
                pk_cols = pk_col_list,
            );

            let ref_full_rows = self
                .ref_client
                .query(&ref_full_sql, &[&chunk_start, &chunk_end])
                .await?;

            // Filter to only rows whose PK is in diff_pks: the key text follows the shared columns
            let diff_rows: Vec<Row> = ref_full_rows
                .iter()
                .filter(|pg_row| diff_pks.contains(&text_key(pg_row, ref_columns_shared.len(), pk_cols.len())))
                .map(|pg_row| extract_row(pg_row, &ref_columns_shared))
                .collect();

            if !diff_rows.is_empty() {
                // Process differing rows through check_rows in chunks
                for chunk in diff_rows.chunks(self.chunk_size) {
                    self.check_rows(
                        chunk,
                        &ref_columns,
                        &target_columns,
                        &pk_cols,
                        &exclude_from_output,
                        insert_writer,
                        update_writer,
                        &mut stats,
                        &mut first_insert,
                        &mut first_update,
                    )
                    .await?;
                }
            }
        }

        Ok(stats)
    }

    /// Build the set of columns to exclude from generated SQL output.
    fn build_exclude_set(
        &self,
        ref_columns: &[ColumnInfo],
        real_pk_names: &[String],
    ) -> HashSet<String> {
        let mut exclude_from_output = self.exclude_columns.clone();
        if self.exclude_real_pk && self.alternate_keys.is_some() {
            for pk_name in real_pk_names {
                exclude_from_output.insert(pk_name.to_lowercase());
            }
        }

        // Exclude identity columns from INSERT if configured
        let exclude_identity = match self.include_identity_columns {
            Some(true) => false,
            Some(false) => true,
            None => true,
        };
        if exclude_identity {
            for col in ref_columns {
                if col.is_identity {
                    exclude_from_output.insert(col.name.to_lowercase());
                }
            }
        }

        exclude_from_output
    }

    /// Check a chunk of reference rows against the target table.
    #[allow(clippy::too_many_arguments)]
    async fn check_rows(
        &self,
        ref_rows: &[Row],
        ref_columns: &[ColumnInfo],
        target_columns: &[ColumnInfo],
        pk_cols: &[ColumnInfo],
        exclude_from_output: &HashSet<String>,
        insert_writer: &mut impl Write,
        update_writer: &mut impl Write,
        stats: &mut DiffStats,
        first_insert: &mut bool,
        first_update: &mut bool,
    ) -> Result<()> {
        // Build check query for the target table
        let check_sql = build_check_query(&self.target_table, target_columns, pk_cols, ref_rows);

        debug!("Check SQL: {}", check_sql);

        let target_rows_result = self.target_client.query(&check_sql, &[]).await?;
        let target_rows: Vec<Row> = target_rows_result
            .iter()
            .map(|r| extract_row(r, target_columns))
            .collect();

        for ref_row in ref_rows {
            if self.cancelled {
                return Err(DataDiffError::Cancelled);
            }

            let target_match = find_row_by_pk(&target_rows, target_columns, ref_row, ref_columns, pk_cols);

            match target_match {
                None => {
                    // Row missing in target -> INSERT
                    if *first_insert {
                        *first_insert = false;
                        write_header(insert_writer, &self.target_table)?;
                    }
                    let sql = generate_insert(
                        &self.target_table,
                        ref_columns,
                        ref_row,
                        exclude_from_output,
                    );
                    writeln!(insert_writer, "{}\n", sql)?;
                    stats.inserts += 1;
                }
                Some(target_row) => {
                    // Row exists -> check for modifications
                    let modified = find_modified_columns(
                        ref_row,
                        target_row,
                        ref_columns,
                        target_columns,
                        &self.columns_to_ignore,
                    );

                    if !modified.is_empty() {
                        if *first_update {
                            *first_update = false;
                            write_header(update_writer, &self.target_table)?;
                        }
                        if let Some(sql) = generate_update(
                            &self.target_table,
                            ref_columns,
                            pk_cols,
                            ref_row,
                            &modified,
                        ) {
                            writeln!(update_writer, "{}\n", sql)?;
                            stats.updates += 1;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

/// Find a row in `target_rows` that matches `ref_row` by PK values.
fn find_row_by_pk<'a>(
    target_rows: &'a [Row],
    target_columns: &[ColumnInfo],
    ref_row: &Row,
    ref_columns: &[ColumnInfo],
    pk_cols: &[ColumnInfo],
) -> Option<&'a Row> {
    target_rows.iter().find(|target_row| {
        pk_cols.iter().all(|pk| {
            let ref_idx = ref_columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(&pk.name))
                .unwrap();
            let target_idx = target_columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(&pk.name))
                .unwrap();
            ref_row[ref_idx] == target_row[target_idx]
        })
    })
}

/// Compare two rows and return the indices of columns that differ.
/// Ignores columns in `columns_to_ignore`.
fn find_modified_columns(
    ref_row: &Row,
    target_row: &Row,
    ref_columns: &[ColumnInfo],
    target_columns: &[ColumnInfo],
    columns_to_ignore: &HashSet<String>,
) -> Vec<usize> {
    let mut modified = Vec::new();

    for (ref_idx, ref_col) in ref_columns.iter().enumerate() {
        if columns_to_ignore.contains(&ref_col.name.to_lowercase()) {
            continue;
        }

        let target_idx = target_columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&ref_col.name));

        if let Some(target_idx) = target_idx
            && ref_row[ref_idx] != target_row[target_idx] {
                modified.push(ref_idx);
            }
    }

    modified
}

fn write_header(writer: &mut impl Write, table_name: &str) -> Result<()> {
    let (_, tname) = parse_table_name(table_name);
    writeln!(
        writer,
        "-- ----------------------------------------------------------------"
    )?;
    writeln!(writer, "-- Data diff for table: {}", tname)?;
    writeln!(
        writer,
        "-- ----------------------------------------------------------------"
    )?;
    Ok(())
}

/// Build a HashMap from MD5 query results: pk_key_string -> md5_hash.
///
/// Each row is expected to have md5 as column 0 and its `pk_len` key columns, as text, from column 1.
fn build_md5_map(rows: &[tokio_postgres::Row], pk_len: usize) -> HashMap<String, String> {
    rows.iter().map(|row| (text_key(row, 1, pk_len), row.get(0))).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::RowValue;
    use postgres_types::Type;

    fn col(name: &str, is_pk: bool) -> ColumnInfo {
        ColumnInfo { name: name.to_string(), pg_type: Type::TEXT, is_pk, is_identity: false, identity_always: false, ordinal: 0 }
    }

    #[test]
    fn a_row_is_found_by_its_key_whatever_the_column_order() {
        let ref_cols = vec![col("id", true), col("name", false)];
        let target_cols = vec![col("NAME", false), col("extra", false), col("ID", true)];
        let pk = vec![col("id", true)];
        let target_rows = vec![
            vec![RowValue::Text("a".into()), RowValue::Null, RowValue::I32(1)],
            vec![RowValue::Text("b".into()), RowValue::Null, RowValue::I32(2)],
        ];
        let found = find_row_by_pk(&target_rows, &target_cols, &vec![RowValue::I32(2), RowValue::Null], &ref_cols, &pk);
        assert_eq!(found, Some(&target_rows[1]));
        assert_eq!(find_row_by_pk(&target_rows, &target_cols, &vec![RowValue::I32(3), RowValue::Null], &ref_cols, &pk), None);
    }

    #[test]
    fn the_modified_columns_skip_the_ignored_and_the_ones_the_target_lacks() {
        let ref_cols = vec![col("id", true), col("name", false), col("Seen", false), col("gone", false)];
        let target_cols = vec![col("id", true), col("name", false), col("seen", false)];
        let ref_row = vec![RowValue::I32(1), RowValue::Text("new".into()), RowValue::I32(5), RowValue::Null];
        let target_row = vec![RowValue::I32(1), RowValue::Text("old".into()), RowValue::I32(4)];
        let ignore: HashSet<String> = ["seen".to_string()].into();
        assert_eq!(find_modified_columns(&ref_row, &target_row, &ref_cols, &target_cols, &ignore), vec![1]);
        assert_eq!(find_modified_columns(&ref_row, &target_row, &ref_cols, &target_cols, &HashSet::new()), vec![1, 2]);
    }

    #[test]
    fn a_header_names_the_table_without_its_schema() {
        let mut out = Vec::new();
        write_header(&mut out, "public.users").unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("-- Data diff for table: users\n"));
        assert_eq!(text.lines().count(), 3);
    }
}

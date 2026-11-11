use std::time::Instant;

use chrono::Duration;
use tokio_postgres::Client;
use tracing::{debug, info};

use crate::chunk_strategy::{determine_strategy, get_row_count};
use crate::config::{parse_table_name, DiffConfig, ProgressEvent, TablePair};
use crate::error::{DataDiffError, Result};
use crate::metadata::get_columns;
use crate::sql_gen::{format_table_name, quote_identifier};
use crate::types::{ChunkStrategy, ColumnInfo, TableAnalysis};

/// Estimate the time (in seconds) to diff a single table based on its strategy and row count.
///
/// For `Md5TimestampChunk`: runs a single chunk MD5 query on both sides, then extrapolates to 10 chunks.
/// For ID-based strategies: uses a heuristic based on row count.
pub async fn estimate_table_time(
    ref_client: &Client,
    target_client: &Client,
    ref_table: &str,
    target_table: &str,
    ref_columns: &[ColumnInfo],
    strategy: &ChunkStrategy,
    row_count: i64,
) -> Result<f64> {
    match strategy {
        ChunkStrategy::Md5TimestampChunk {
            timestamp_column,
            min_ts,
            max_ts: _,
            chunk_days,
        } => {
            // Time a single chunk query on both sides
            let chunk_end = *min_ts + Duration::seconds((*chunk_days * 86400.0) as i64);

            let col_list = build_row_column_list(ref_columns);
            let ref_sql = format!(
                "SELECT md5(ROW({})::text) FROM {} WHERE {} >= $1 AND {} < $2",
                col_list,
                format_table_name(ref_table),
                quote_identifier(timestamp_column),
                quote_identifier(timestamp_column),
            );

            let start = Instant::now();
            let _ = ref_client
                .query(&ref_sql, &[min_ts, &chunk_end])
                .await?;
            let ref_time = start.elapsed().as_secs_f64();

            let target_col_list = build_row_column_list(ref_columns);
            let target_sql = format!(
                "SELECT md5(ROW({})::text) FROM {} WHERE {} >= $1 AND {} < $2",
                target_col_list,
                format_table_name(target_table),
                quote_identifier(timestamp_column),
                quote_identifier(timestamp_column),
            );

            let start = Instant::now();
            let _ = target_client
                .query(&target_sql, &[min_ts, &chunk_end])
                .await?;
            let target_time = start.elapsed().as_secs_f64();

            let chunk_time = ref_time + target_time;
            // Extrapolate to 10 chunks
            let estimated = chunk_time * 10.0;
            debug!(
                "Table {} ETC: chunk_time={:.2}s, estimated={:.2}s",
                ref_table, chunk_time, estimated
            );
            Ok(estimated)
        }
        _ => {
            // Heuristic: ~50ms per 1000 rows for ID-based comparison
            let estimated = (row_count as f64 / 1000.0) * 0.05;
            Ok(estimated.max(0.1))
        }
    }
}

/// Build a comma-separated list of quoted column names for use in `ROW(col1, col2, ...)`.
fn build_row_column_list(columns: &[ColumnInfo]) -> String {
    columns
        .iter()
        .map(|c| quote_identifier(&c.name))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Analyze all table pairs and estimate total time to completion.
///
/// For each table:
/// 1. Gets column info from the reference database
/// 2. Determines chunk strategy (row count, timestamp detection)
/// 3. Estimates per-table time
///
/// If total estimated time exceeds `config.max_etc_seconds` and `!config.force`,
/// returns a `Config` error with the ETC.
pub async fn analyze_and_estimate(
    ref_client: &Client,
    target_client: &Client,
    table_pairs: &[TablePair],
    config: &DiffConfig,
) -> Result<Vec<TableAnalysis>> {
    let mut analyses = Vec::with_capacity(table_pairs.len());

    config.emit_progress(ProgressEvent::EstimationStart {
        total_tables: table_pairs.len(),
    });

    for pair in table_pairs {
        let ref_columns = get_columns(ref_client, &pair.reference).await?;

        let (_, ref_tname) = parse_table_name(&pair.reference);
        let is_excluded = config
            .exclude_tables
            .contains(&ref_tname.to_lowercase());

        let (strategy, ts_col) = determine_strategy(
            ref_client,
            &pair.reference,
            &ref_columns,
            config.timestamp_column.as_deref(),
            is_excluded,
        )
        .await?;

        let row_count = get_row_count(ref_client, &pair.reference).await?;

        let estimated_seconds = estimate_table_time(
            ref_client,
            target_client,
            &pair.reference,
            &pair.target,
            &ref_columns,
            &strategy,
            row_count,
        )
        .await?;

        config.emit_progress(ProgressEvent::EstimationTableDone {
            table: ref_tname.to_string(),
            estimated_seconds,
            strategy: strategy.to_string(),
        });

        analyses.push(TableAnalysis {
            ref_table: pair.reference.clone(),
            target_table: pair.target.clone(),
            row_count,
            timestamp_column: ts_col,
            strategy,
            estimated_seconds,
        });
    }

    let total_etc: f64 = analyses.iter().map(|a| a.estimated_seconds).sum();

    config.emit_progress(ProgressEvent::EstimationComplete {
        total_estimated_seconds: total_etc,
    });

    if total_etc > config.max_etc_seconds && !config.force {
        return Err(DataDiffError::Config(tr!(
            "error.too_long",
            seconds = format!("{:.0}", total_etc),
            hours = format!("{:.1}", total_etc / 3600.0),
            max = format!("{:.0}", config.max_etc_seconds),
        )));
    }

    info!(
        "ETC estimation complete: {:.1}s for {} tables",
        total_etc,
        analyses.len()
    );

    Ok(analyses)
}

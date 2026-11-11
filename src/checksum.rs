use std::collections::HashMap;
use std::path::Path;

use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
use tokio_postgres::Client;
use tracing::debug;

use crate::config::parse_table_name;
use crate::error::{DataDiffError, Result};
use crate::sql_gen::{format_table_name, pk_text_list, quote_identifier};
use crate::types::{text_key, ChunkStrategy, ColumnInfo, TableAnalysis};

/// Checksum data for a single table chunk.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ChunkChecksum {
    pub chunk_index: usize,
    pub chunk_start: String,
    pub chunk_end: String,
    pub row_hashes: HashMap<String, String>,
}

/// Checksum data for a single table.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TableChecksum {
    pub table_name: String,
    pub strategy: String,
    pub row_count: i64,
    pub timestamp_column: Option<String>,
    pub chunks: Vec<ChunkChecksum>,
}

/// Top-level checksum manifest for a database.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ChecksumManifest {
    pub db_name: Option<String>,
    pub schema: Option<String>,
    pub created_at: String,
    pub tables: Vec<TableChecksum>,
}

/// Summary of differences between two tables' checksums.
#[derive(Debug, Clone)]
pub struct TableDiffSummary {
    pub table_name: String,
    pub differing_chunks: Vec<usize>,
    pub missing_in_target: Vec<String>,
    pub missing_in_reference: Vec<String>,
    pub modified: Vec<String>,
}

/// Compute MD5 checksums for all chunks of a single table, each row's keyed by its key as text.
///
/// A table without a key has no row to key a checksum by: `NoPrimaryKey`.
pub async fn compute_table_checksums(
    client: &Client,
    table_name: &str,
    columns: &[ColumnInfo],
    pk_cols: &[ColumnInfo],
    analysis: &TableAnalysis,
) -> Result<TableChecksum> {
    if pk_cols.is_empty() {
        return Err(DataDiffError::NoPrimaryKey(table_name.to_string()));
    }
    let table_sql = format_table_name(table_name);
    let col_list: String = columns
        .iter()
        .map(|c| quote_identifier(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let pk_col_list: String = pk_cols
        .iter()
        .map(|c| quote_identifier(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let pk_text = pk_text_list(pk_cols);

    let chunks = match &analysis.strategy {
        ChunkStrategy::Md5TimestampChunk {
            timestamp_column,
            min_ts,
            max_ts,
            chunk_days,
        } => {
            let ts_col_sql = quote_identifier(timestamp_column);
            let total_duration = max_ts.signed_duration_since(*min_ts);
            let total_days = total_duration.num_seconds() as f64 / 86400.0;
            let num_chunks = if total_days > 0.0 {
                (total_days / chunk_days).ceil() as usize
            } else {
                1
            };

            let mut chunks = Vec::with_capacity(num_chunks);
            for chunk_idx in 0..num_chunks {
                let chunk_start =
                    *min_ts + Duration::seconds((chunk_idx as f64 * chunk_days * 86400.0) as i64);
                let chunk_end = if chunk_idx == num_chunks - 1 {
                    *max_ts + Duration::seconds(1)
                } else {
                    *min_ts
                        + Duration::seconds(((chunk_idx + 1) as f64 * chunk_days * 86400.0) as i64)
                };

                let sql = format!(
                    "SELECT md5(ROW({col_list})::text), {pk_text} FROM {table} \
                     WHERE {ts} >= $1 AND {ts} < $2 ORDER BY {pk_cols}",
                    col_list = col_list,
                    pk_text = pk_text,
                    pk_cols = pk_col_list,
                    table = table_sql,
                    ts = ts_col_sql,
                );

                let rows = client.query(&sql, &[&chunk_start, &chunk_end]).await?;

                let mut row_hashes = HashMap::new();
                for row in &rows {
                    let md5: String = row.get(0);
                    let pk_key = extract_pk_key_from_row(row, pk_cols);
                    row_hashes.insert(pk_key, md5);
                }

                chunks.push(ChunkChecksum {
                    chunk_index: chunk_idx,
                    chunk_start: chunk_start.to_string(),
                    chunk_end: chunk_end.to_string(),
                    row_hashes,
                });
            }
            chunks
        }
        _ => {
            // For IdChunkSmall/Medium/LargeExcluded: single chunk with all rows
            let sql = format!(
                "SELECT md5(ROW({col_list})::text), {pk_text} FROM {table} ORDER BY {pk_cols}",
                col_list = col_list,
                pk_text = pk_text,
                pk_cols = pk_col_list,
                table = table_sql,
            );

            let rows = client.query(&sql, &[]).await?;

            let mut row_hashes = HashMap::new();
            for row in &rows {
                let md5: String = row.get(0);
                let pk_key = extract_pk_key_from_row(row, pk_cols);
                row_hashes.insert(pk_key, md5);
            }

            vec![ChunkChecksum {
                chunk_index: 0,
                chunk_start: String::new(),
                chunk_end: String::new(),
                row_hashes,
            }]
        }
    };

    let (_, tname) = parse_table_name(table_name);

    Ok(TableChecksum {
        table_name: tname.to_string(),
        strategy: analysis.strategy.to_string(),
        row_count: analysis.row_count,
        timestamp_column: analysis.timestamp_column.clone(),
        chunks,
    })
}

/// Compute checksums for all tables in a database. A table without a key stops it (`NoPrimaryKey`):
/// leave those out, as `DataDiff` does.
pub async fn compute_db_checksums(
    client: &Client,
    tables: &[(String, Vec<ColumnInfo>, Vec<ColumnInfo>)],
    analyses: &[TableAnalysis],
    db_name: Option<&str>,
    schema: Option<&str>,
) -> Result<ChecksumManifest> {
    let analysis_map: HashMap<&str, &TableAnalysis> =
        analyses.iter().map(|a| (a.ref_table.as_str(), a)).collect();

    let mut table_checksums = Vec::new();
    for (table_name, columns, pk_cols) in tables {
        if let Some(analysis) = analysis_map.get(table_name.as_str()) {
            let tc = compute_table_checksums(client, table_name, columns, pk_cols, analysis).await?;
            table_checksums.push(tc);
        }
    }

    Ok(ChecksumManifest {
        db_name: db_name.map(|s| s.to_string()),
        schema: schema.map(|s| s.to_string()),
        created_at: Utc::now().to_rfc3339(),
        tables: table_checksums,
    })
}

/// Serialize a checksum manifest to a YAML file.
pub fn save_checksums(manifest: &ChecksumManifest, path: &Path) -> Result<()> {
    let mut file = std::io::BufWriter::new(std::fs::File::create(path)?);
    serde_saphyr::to_io_writer(&mut file, manifest)?;
    std::io::Write::flush(&mut file)?;
    debug!("Saved checksums to {}", path.display());
    Ok(())
}

/// [yaml] How a checksum file is read: a manifest holds a hash per row, so serde-saphyr's default limits
/// (250,000 nodes, a million events, 256 MB) would refuse a table of some 125,000 rows that serde_yaml
/// (0.4.0) wrote and read. The file is the operator's own: no limit on its size, the nesting and the
/// alias limits kept (0.4.1, 2026-11-11)
fn read_options() -> serde_saphyr::Options {
    let mut budget = serde_saphyr::Budget::default();
    budget.max_reader_input_bytes = None;
    budget.max_events = usize::MAX;
    budget.max_nodes = usize::MAX;
    budget.max_total_scalar_bytes = usize::MAX;
    let mut options = serde_saphyr::Options::default();
    options.budget = Some(budget);
    options
}

/// Deserialize a checksum manifest from a YAML file.
pub fn load_checksums(path: &Path) -> Result<ChecksumManifest> {
    let file = std::fs::File::open(path).map_err(|e| {
        DataDiffError::Checksum(tr!("error.checksum_open", path = path.display(), err = e))
    })?;
    let manifest: ChecksumManifest = serde_saphyr::from_reader_with_options(std::io::BufReader::new(file), read_options())?;
    debug!("Loaded checksums from {}", path.display());
    Ok(manifest)
}

/// Compare two checksum manifests and return per-table diff summaries.
pub fn compare_checksums(
    ref_manifest: &ChecksumManifest,
    target_manifest: &ChecksumManifest,
) -> Vec<TableDiffSummary> {
    let target_map: HashMap<&str, &TableChecksum> = target_manifest
        .tables
        .iter()
        .map(|t| (t.table_name.as_str(), t))
        .collect();

    let mut summaries = Vec::new();

    for ref_table in &ref_manifest.tables {
        let target_table = match target_map.get(ref_table.table_name.as_str()) {
            Some(t) => t,
            None => {
                // Entire table missing in target - all ref PKs are missing
                let all_pks: Vec<String> = ref_table
                    .chunks
                    .iter()
                    .flat_map(|c| c.row_hashes.keys().cloned())
                    .collect();
                summaries.push(TableDiffSummary {
                    table_name: ref_table.table_name.clone(),
                    differing_chunks: (0..ref_table.chunks.len()).collect(),
                    missing_in_target: all_pks,
                    missing_in_reference: Vec::new(),
                    modified: Vec::new(),
                });
                continue;
            }
        };

        let mut differing_chunks = Vec::new();
        let mut missing_in_target = Vec::new();
        let mut missing_in_reference = Vec::new();
        let mut modified = Vec::new();

        // Build a map of target chunks by index for lookup
        let target_chunk_map: HashMap<usize, &ChunkChecksum> = target_table
            .chunks
            .iter()
            .map(|c| (c.chunk_index, c))
            .collect();

        for ref_chunk in &ref_table.chunks {
            let target_chunk = target_chunk_map.get(&ref_chunk.chunk_index);

            let target_hashes: &HashMap<String, String> = match target_chunk {
                Some(tc) => &tc.row_hashes,
                None => {
                    // Entire chunk missing in target
                    differing_chunks.push(ref_chunk.chunk_index);
                    missing_in_target.extend(ref_chunk.row_hashes.keys().cloned());
                    continue;
                }
            };

            let mut chunk_differs = false;

            // Check ref rows against target
            for (pk, ref_hash) in &ref_chunk.row_hashes {
                match target_hashes.get(pk) {
                    None => {
                        missing_in_target.push(pk.clone());
                        chunk_differs = true;
                    }
                    Some(target_hash) if target_hash != ref_hash => {
                        modified.push(pk.clone());
                        chunk_differs = true;
                    }
                    _ => {}
                }
            }

            // Check for rows in target but not in ref
            for pk in target_hashes.keys() {
                if !ref_chunk.row_hashes.contains_key(pk) {
                    missing_in_reference.push(pk.clone());
                    chunk_differs = true;
                }
            }

            if chunk_differs {
                differing_chunks.push(ref_chunk.chunk_index);
            }
        }

        // Check for chunks in target that aren't in ref
        for target_chunk in &target_table.chunks {
            let in_ref = ref_table
                .chunks
                .iter()
                .any(|c| c.chunk_index == target_chunk.chunk_index);
            if !in_ref {
                differing_chunks.push(target_chunk.chunk_index);
                missing_in_reference.extend(target_chunk.row_hashes.keys().cloned());
            }
        }

        if !differing_chunks.is_empty()
            || !missing_in_target.is_empty()
            || !missing_in_reference.is_empty()
            || !modified.is_empty()
        {
            summaries.push(TableDiffSummary {
                table_name: ref_table.table_name.clone(),
                differing_chunks,
                missing_in_target,
                missing_in_reference,
                modified,
            });
        }
    }

    summaries
}

/// Extract a PK key string from a tokio_postgres::Row whose PK columns, as text, start at index 1.
fn extract_pk_key_from_row(row: &tokio_postgres::Row, pk_cols: &[ColumnInfo]) -> String {
    text_key(row, 1, pk_cols.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checksum_manifest_roundtrip() {
        let manifest = ChecksumManifest {
            db_name: Some("test_db".to_string()),
            schema: Some("public".to_string()),
            created_at: "2025-01-01T00:00:00Z".to_string(),
            tables: vec![TableChecksum {
                table_name: "users".to_string(),
                strategy: "IdChunkSmall".to_string(),
                row_count: 100,
                timestamp_column: None,
                chunks: vec![ChunkChecksum {
                    chunk_index: 0,
                    chunk_start: String::new(),
                    chunk_end: String::new(),
                    row_hashes: HashMap::from([
                        ("1".to_string(), "abc123".to_string()),
                        ("2".to_string(), "def456".to_string()),
                    ]),
                }],
            }],
        };

        let yaml = serde_saphyr::to_string(&manifest).unwrap();
        let deserialized: ChecksumManifest = serde_saphyr::from_str(&yaml).unwrap();

        assert_eq!(deserialized.db_name, manifest.db_name);
        assert_eq!(deserialized.schema, manifest.schema);
        assert_eq!(deserialized.tables.len(), 1);
        assert_eq!(deserialized.tables[0].table_name, "users");
        assert_eq!(deserialized.tables[0].chunks[0].row_hashes.len(), 2);
    }

    #[test]
    fn test_compare_checksums_with_diffs() {
        let ref_manifest = ChecksumManifest {
            db_name: None,
            schema: None,
            created_at: String::new(),
            tables: vec![TableChecksum {
                table_name: "users".to_string(),
                strategy: "IdChunkSmall".to_string(),
                row_count: 3,
                timestamp_column: None,
                chunks: vec![ChunkChecksum {
                    chunk_index: 0,
                    chunk_start: String::new(),
                    chunk_end: String::new(),
                    row_hashes: HashMap::from([
                        ("1".to_string(), "aaa".to_string()),
                        ("2".to_string(), "bbb".to_string()),
                        ("3".to_string(), "ccc".to_string()),
                    ]),
                }],
            }],
        };

        let target_manifest = ChecksumManifest {
            db_name: None,
            schema: None,
            created_at: String::new(),
            tables: vec![TableChecksum {
                table_name: "users".to_string(),
                strategy: "IdChunkSmall".to_string(),
                row_count: 3,
                timestamp_column: None,
                chunks: vec![ChunkChecksum {
                    chunk_index: 0,
                    chunk_start: String::new(),
                    chunk_end: String::new(),
                    row_hashes: HashMap::from([
                        ("1".to_string(), "aaa".to_string()),     // same
                        ("2".to_string(), "xxx".to_string()),     // modified
                        ("4".to_string(), "ddd".to_string()),     // extra in target
                    ]),
                }],
            }],
        };

        let summaries = compare_checksums(&ref_manifest, &target_manifest);
        assert_eq!(summaries.len(), 1);

        let summary = &summaries[0];
        assert_eq!(summary.table_name, "users");
        assert_eq!(summary.differing_chunks, vec![0]);
        assert!(summary.missing_in_target.contains(&"3".to_string()));
        assert!(summary.missing_in_reference.contains(&"4".to_string()));
        assert!(summary.modified.contains(&"2".to_string()));
    }

    #[test]
    fn test_compare_checksums_identical() {
        let manifest = ChecksumManifest {
            db_name: None,
            schema: None,
            created_at: String::new(),
            tables: vec![TableChecksum {
                table_name: "orders".to_string(),
                strategy: "IdChunkSmall".to_string(),
                row_count: 2,
                timestamp_column: None,
                chunks: vec![ChunkChecksum {
                    chunk_index: 0,
                    chunk_start: String::new(),
                    chunk_end: String::new(),
                    row_hashes: HashMap::from([
                        ("1".to_string(), "aaa".to_string()),
                        ("2".to_string(), "bbb".to_string()),
                    ]),
                }],
            }],
        };

        let summaries = compare_checksums(&manifest, &manifest);
        assert!(summaries.is_empty());
    }

    fn chunk(index: usize, rows: &[(&str, &str)]) -> ChunkChecksum {
        ChunkChecksum {
            chunk_index: index,
            chunk_start: String::new(),
            chunk_end: String::new(),
            row_hashes: rows.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
        }
    }

    fn manifest(tables: Vec<(&str, Vec<ChunkChecksum>)>) -> ChecksumManifest {
        ChecksumManifest {
            db_name: None,
            schema: None,
            created_at: String::new(),
            tables: tables
                .into_iter()
                .map(|(name, chunks)| TableChecksum {
                    table_name: name.to_string(),
                    strategy: "Md5TimestampChunk".to_string(),
                    row_count: 0,
                    timestamp_column: Some("updated_at".to_string()),
                    chunks,
                })
                .collect(),
        }
    }

    #[test]
    fn a_table_or_a_chunk_on_one_side_only_differs_whole() {
        let reference = manifest(vec![
            ("gone", vec![chunk(0, &[("1", "a")]), chunk(1, &[("2", "b")])]),
            ("events", vec![chunk(0, &[("1", "a")]), chunk(1, &[("2", "b")])]),
        ]);
        let target = manifest(vec![("events", vec![chunk(0, &[("1", "a")]), chunk(2, &[("3", "c")])])]);
        let mut summaries = compare_checksums(&reference, &target);
        summaries.sort_by(|a, b| a.table_name.cmp(&b.table_name));
        assert_eq!(summaries.len(), 2);

        let events = &summaries[0];
        assert_eq!(events.table_name, "events");
        assert_eq!(events.differing_chunks, vec![1, 2]);
        assert_eq!(events.missing_in_target, vec!["2"]);
        assert_eq!(events.missing_in_reference, vec!["3"]);
        assert!(events.modified.is_empty());

        let gone = &summaries[1];
        assert_eq!(gone.differing_chunks, vec![0, 1]);
        let mut missing = gone.missing_in_target.clone();
        missing.sort();
        assert_eq!(missing, vec!["1", "2"]);
    }

    #[test]
    fn a_manifest_is_saved_and_loaded_back_and_a_bad_file_refused() {
        let dir = std::env::temp_dir().join(format!("diff_data_checksum_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("ref.yaml");
        let saved = manifest(vec![("events", vec![chunk(0, &[("1", "a")])])]);
        save_checksums(&saved, &path).unwrap();
        let loaded = load_checksums(&path).unwrap();
        assert_eq!(loaded.tables[0].table_name, "events");
        assert_eq!(loaded.tables[0].chunks[0].row_hashes["1"], "a");

        match load_checksums(&dir.join("missing.yaml")) {
            Err(DataDiffError::Checksum(msg)) => assert!(msg.starts_with("DA-E017 "), "{}", msg),
            other => panic!("{:?}", other),
        }
        let bad = dir.join("bad.yaml");
        std::fs::write(&bad, "tables: [").unwrap();
        assert!(matches!(load_checksums(&bad), Err(DataDiffError::Yaml(_))));
        assert!(save_checksums(&saved, &dir.join("no/such/dir.yaml")).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// [yaml] a checksum file as 0.4.0 (serde_yaml) wrote it - its quoted keys and values, `null`, `''`
    /// and unquoted timestamps - read by 0.4.1 (serde-saphyr) as it was meant (2026-11-11)
    const MANIFEST_0_4_0: &str = r#"db_name: shop
schema: null
created_at: 2026-11-11T19:00:00+00:00
tables:
- table_name: public.orders
  strategy: IdChunkSmall
  row_count: 3
  timestamp_column: null
  chunks:
  - chunk_index: 0
    chunk_start: ''
    chunk_end: ''
    row_hashes:
      '1': 0cc175b9c0f1b6a831c399e269772661
- table_name: public.events
  strategy: Md5TimestampChunked
  row_count: 0
  timestamp_column: updated_at
  chunks:
  - chunk_index: 2
    chunk_start: 2026-11-01 00:00:00+00
    chunk_end: 2026-11-02 00:00:00+00
    row_hashes:
      '007': 'true'
"#;

    #[test]
    fn a_checksum_file_0_4_0_wrote_loads_in_0_4_1() {
        let dir = std::env::temp_dir().join(format!("diff_data_checksum_040_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("ref.yaml");
        std::fs::write(&path, MANIFEST_0_4_0).unwrap();
        let m = load_checksums(&path).unwrap();
        assert_eq!((m.db_name.as_deref(), m.schema.as_deref()), (Some("shop"), None));
        assert_eq!(m.created_at, "2026-11-11T19:00:00+00:00", "a timestamp is read as the text it is");
        assert_eq!(m.tables.len(), 2);
        let (o, e) = (&m.tables[0], &m.tables[1]);
        assert_eq!((o.table_name.as_str(), o.strategy.as_str(), o.row_count, o.timestamp_column.as_deref()),
            ("public.orders", "IdChunkSmall", 3, None));
        assert_eq!((o.chunks[0].chunk_start.as_str(), o.chunks[0].chunk_end.as_str()), ("", ""));
        assert_eq!(o.chunks[0].row_hashes["1"], "0cc175b9c0f1b6a831c399e269772661", "a key '1' stays the text 1");
        assert_eq!(e.timestamp_column.as_deref(), Some("updated_at"));
        assert_eq!((e.chunks[0].chunk_index, e.chunks[0].chunk_start.as_str()), (2, "2026-11-01 00:00:00+00"));
        assert_eq!(e.chunks[0].row_hashes["007"], "true", "'007' and 'true' stay texts");
        // and 0.4.1 writes what it reads back the same
        save_checksums(&m, &path).unwrap();
        let again = load_checksums(&path).unwrap();
        assert_eq!(serde_saphyr::to_string(&again).unwrap(), serde_saphyr::to_string(&m).unwrap());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// [yaml] a checksum file over serde-saphyr's default limits (250,000 nodes): a table of 200,000 rows
    /// loads, as it did with serde_yaml (2026-11-11)
    #[test]
    fn a_checksum_file_of_200_000_rows_loads() {
        let dir = std::env::temp_dir().join(format!("diff_data_checksum_big_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("big.yaml");
        let rows: Vec<(String, String)> = (0..200_000).map(|i| (i.to_string(), format!("{:032x}", i))).collect();
        let pairs: Vec<(&str, &str)> = rows.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let saved = manifest(vec![("events", vec![chunk(0, &pairs)])]);
        save_checksums(&saved, &path).unwrap();
        let loaded = load_checksums(&path).unwrap();
        assert_eq!(loaded.tables[0].chunks[0].row_hashes.len(), 200_000);
        assert_eq!(loaded.tables[0].chunks[0].row_hashes["199999"], format!("{:032x}", 199_999));
        let _ = std::fs::remove_dir_all(&dir);
    }
}

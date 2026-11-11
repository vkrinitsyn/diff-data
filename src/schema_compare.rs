use std::collections::{HashMap, HashSet};

use schema_guard_tokio::loader::{load_info_schema, InfoSchemaType, PgTable};
use tracing::debug;

use crate::config::PgPool;
use crate::error::{DataDiffError, Result};
use crate::types::SchemaComparisonResult;

/// Load the full info schema from a connection pool using `schema_guard_tokio`.
pub async fn load_schema_from_pool(
    pool: &PgPool,
    db_name: &str,
) -> Result<InfoSchemaType> {
    let mut conn = pool.get().await?;
    let mut txn = conn
        .transaction()
        .await
        .map_err(DataDiffError::Postgres)?;
    let info = load_info_schema(db_name, &mut txn)
        .await
        .map_err(|e| DataDiffError::Config(format!("Failed to load info schema: {}", e)))?;
    txn.commit().await.map_err(DataDiffError::Postgres)?;
    Ok(info)
}

/// Compare the schema of a single table between reference and target.
///
/// Returns a `SchemaComparisonResult` indicating whether schemas match,
/// the target is a superset (has extra columns), the table is excluded, or there is a mismatch.
pub fn compare_table_schema(
    ref_table: &PgTable,
    target_table: &PgTable,
    is_excluded: bool,
) -> SchemaComparisonResult {
    let ref_cols = &ref_table.columns;
    let target_cols = &target_table.columns;

    // Check if all reference columns exist in target with matching type and nullability
    let mut missing_in_target = Vec::new();
    let mut type_mismatches = Vec::new();

    for (col_name, ref_def) in ref_cols {
        match target_cols.get(col_name) {
            None => {
                missing_in_target.push(col_name.clone());
            }
            Some(target_def) => {
                if ref_def.column_type != target_def.column_type {
                    type_mismatches.push(format!(
                        "column '{}': type '{}' vs '{}'",
                        col_name, ref_def.column_type, target_def.column_type
                    ));
                }
            }
        }
    }

    // If reference columns are missing from target, it's a mismatch
    if !missing_in_target.is_empty() {
        let msg = format!(
            "Reference columns missing in target: {}",
            missing_in_target.join(", ")
        );
        if is_excluded {
            return SchemaComparisonResult::Excluded;
        }
        return SchemaComparisonResult::Mismatch { message: msg };
    }

    // If types differ, it's a mismatch
    if !type_mismatches.is_empty() {
        let msg = format!("Type mismatches: {}", type_mismatches.join("; "));
        if is_excluded {
            return SchemaComparisonResult::Excluded;
        }
        return SchemaComparisonResult::Mismatch { message: msg };
    }

    // Check if target has extra columns (superset)
    let extra_columns: Vec<String> = target_cols
        .keys()
        .filter(|col_name| !ref_cols.contains_key(*col_name))
        .cloned()
        .collect();

    if !extra_columns.is_empty() {
        return SchemaComparisonResult::TargetSuperset { extra_columns };
    }

    SchemaComparisonResult::Match
}

/// Compare schemas for all table pairs between reference and target databases.
///
/// Returns a map of `table_name -> SchemaComparisonResult` for each table found.
pub async fn compare_all_schemas(
    ref_pool: &PgPool,
    target_pool: &PgPool,
    ref_db_name: &str,
    target_db_name: &str,
    ref_schema: &str,
    target_schema: &str,
    exclude_tables: &HashSet<String>,
) -> Result<HashMap<String, SchemaComparisonResult>> {
    let ref_info = load_schema_from_pool(ref_pool, ref_db_name).await?;
    let target_info = load_schema_from_pool(target_pool, target_db_name).await?;

    let ref_tables = ref_info.get(ref_schema).cloned().unwrap_or_default();
    let target_tables = target_info.get(target_schema).cloned().unwrap_or_default();

    let mut results = HashMap::new();

    for (table_name, ref_table) in &ref_tables {
        let is_excluded = exclude_tables.contains(&table_name.to_lowercase());

        match target_tables.get(table_name) {
            None => {
                if is_excluded {
                    results.insert(table_name.clone(), SchemaComparisonResult::Excluded);
                } else {
                    results.insert(
                        table_name.clone(),
                        SchemaComparisonResult::Mismatch {
                            message: format!("Table '{}' not found in target schema", table_name),
                        },
                    );
                }
            }
            Some(target_table) => {
                let result = compare_table_schema(ref_table, target_table, is_excluded);
                debug!("Schema compare {}: {:?}", table_name, result);
                results.insert(table_name.clone(), result);
            }
        }
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use schema_guard_tokio::loader::PgColumnDfn;

    fn table(cols: &[(&str, &str)]) -> PgTable {
        PgTable {
            table_name: "t".into(),
            columns: cols
                .iter()
                .enumerate()
                .map(|(i, (n, ty))| {
                    (n.to_string(), PgColumnDfn {
                        column_name: n.to_string(),
                        column_type: ty.to_string(),
                        column_default: None,
                        sql: None,
                        fk: None,
                        pk: i == 0,
                        nullable: true,
                        sort_order: i,
                        column_comment: None,
                    })
                })
                .collect(),
            fks: HashMap::new(),
            triggers: HashMap::new(),
            indexes: HashMap::new(),
            grants: HashMap::new(),
            primary_key: None,
            sort_order: 0,
            table_comment: None,
            owner: None,
        }
    }

    #[test]
    fn a_target_matches_has_more_or_mismatches() {
        let base = table(&[("id", "int4"), ("name", "text")]);
        assert!(matches!(compare_table_schema(&base, &base, false), SchemaComparisonResult::Match));

        let wider = table(&[("id", "int4"), ("name", "text"), ("extra", "int8")]);
        match compare_table_schema(&base, &wider, false) {
            SchemaComparisonResult::TargetSuperset { extra_columns } => assert_eq!(extra_columns, vec!["extra"]),
            other => panic!("{:?}", other),
        }

        let narrower = table(&[("id", "int4")]);
        match compare_table_schema(&base, &narrower, false) {
            SchemaComparisonResult::Mismatch { message } => {
                assert_eq!(message, "Reference columns missing in target: name")
            }
            other => panic!("{:?}", other),
        }
        assert!(matches!(compare_table_schema(&base, &narrower, true), SchemaComparisonResult::Excluded));

        let retyped = table(&[("id", "int8"), ("name", "text")]);
        match compare_table_schema(&base, &retyped, false) {
            SchemaComparisonResult::Mismatch { message } => {
                assert_eq!(message, "Type mismatches: column 'id': type 'int4' vs 'int8'")
            }
            other => panic!("{:?}", other),
        }
        assert!(matches!(compare_table_schema(&base, &retyped, true), SchemaComparisonResult::Excluded));
    }
}

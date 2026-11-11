use std::collections::{BTreeSet, HashMap, HashSet};

use postgres_types::Type;
use tokio_postgres::Client;

use crate::config::parse_table_name;
use crate::error::{DataDiffError, Result};
use crate::types::ColumnInfo;

/// Map a PostgreSQL type OID + type name to a `postgres_types::Type`.
fn resolve_pg_type(type_name: &str) -> Type {
    match type_name {
        "bool" | "boolean" => Type::BOOL,
        "int2" | "smallint" => Type::INT2,
        "int4" | "integer" | "int" | "serial" => Type::INT4,
        "int8" | "bigint" | "bigserial" => Type::INT8,
        "float4" | "real" => Type::FLOAT4,
        "float8" | "double precision" => Type::FLOAT8,
        "numeric" | "decimal" => Type::NUMERIC,
        "text" => Type::TEXT,
        "varchar" | "character varying" => Type::VARCHAR,
        "char" | "character" | "bpchar" => Type::BPCHAR,
        "bytea" => Type::BYTEA,
        "timestamp" | "timestamp without time zone" => Type::TIMESTAMP,
        "timestamptz" | "timestamp with time zone" => Type::TIMESTAMPTZ,
        "date" => Type::DATE,
        "time" | "time without time zone" => Type::TIME,
        "uuid" => Type::UUID,
        "json" => Type::JSON,
        "jsonb" => Type::JSONB,
        "oid" => Type::OID,
        "name" => Type::NAME,
        _ => Type::TEXT, // fallback
    }
}

/// Resolve the schema name, falling back to the current schema if not provided.
async fn resolve_schema(client: &Client, schema_opt: Option<&str>) -> Result<String> {
    if let Some(s) = schema_opt {
        Ok(s.to_string())
    } else {
        let row = client.query_one("SELECT current_schema()", &[]).await?;
        Ok(row.get(0))
    }
}

/// Retrieve column metadata for a table, including PK and identity information.
///
/// Queries `information_schema.columns` for column details and
/// `pg_constraint` + `pg_attribute` for primary key membership.
pub async fn get_columns(
    client: &Client,
    schema_qualified_name: &str,
) -> Result<Vec<ColumnInfo>> {
    let (schema_opt, table_name) = parse_table_name(schema_qualified_name);
    let schema = resolve_schema(client, schema_opt).await?;

    // Get column info from information_schema
    let col_rows = client
        .query(
            "SELECT column_name, udt_name, ordinal_position, is_identity, \
             column_default, identity_generation \
             FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2 \
             ORDER BY ordinal_position",
            &[&schema, &table_name],
        )
        .await?;

    if col_rows.is_empty() {
        return Err(DataDiffError::TableNotFound(
            schema_qualified_name.to_string(),
        ));
    }

    // Get PK columns from pg_constraint
    let pk_rows = client
        .query(
            "SELECT a.attname \
             FROM pg_constraint c \
             JOIN pg_namespace n ON n.oid = c.connamespace \
             JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey) \
             WHERE c.contype = 'p' \
             AND n.nspname = $1 \
             AND c.conrelid = (quote_ident($1) || '.' || quote_ident($2))::regclass",
            &[&schema, &table_name],
        )
        .await?;

    let pk_names: Vec<String> = pk_rows.iter().map(|r| r.get::<_, String>(0)).collect();

    let columns: Vec<ColumnInfo> = col_rows
        .iter()
        .enumerate()
        .map(|(idx, row)| {
            let name: String = row.get("column_name");
            let udt_name: String = row.get("udt_name");
            let is_identity_str: String = row.get("is_identity");
            let col_default: Option<String> = row.get("column_default");
            let identity_generation: Option<String> = row.get("identity_generation");

            let is_pk = pk_names.iter().any(|pk| pk.eq_ignore_ascii_case(&name));
            let is_identity = is_identity_str == "YES"
                || col_default
                    .as_deref()
                    .map(|d| d.starts_with("nextval("))
                    .unwrap_or(false);

            ColumnInfo {
                name,
                pg_type: resolve_pg_type(&udt_name),
                is_pk,
                is_identity,
                identity_always: identity_generation.as_deref() == Some("ALWAYS"),
                ordinal: idx,
            }
        })
        .collect();

    Ok(columns)
}

/// Check if a table exists.
pub async fn table_exists(client: &Client, schema_qualified_name: &str) -> Result<bool> {
    let (schema_opt, table_name) = parse_table_name(schema_qualified_name);
    let schema = resolve_schema(client, schema_opt).await?;

    let row = client
        .query_one(
            "SELECT EXISTS ( \
             SELECT 1 FROM information_schema.tables \
             WHERE table_schema = $1 AND table_name = $2)",
            &[&schema, &table_name],
        )
        .await?;

    Ok(row.get::<_, bool>(0))
}

/// List all table names in a given schema.
pub async fn list_tables(client: &Client, schema: &str) -> Result<Vec<String>> {
    let rows = client
        .query(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = $1 AND table_type = 'BASE TABLE' \
             ORDER BY table_name",
            &[&schema],
        )
        .await?;

    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

/// Foreign key dependency: `table` depends on `references_table`.
#[derive(Debug, Clone)]
pub struct FkDependency {
    pub table: String,
    pub references_table: String,
}

/// Retrieve foreign key dependencies for all tables in a schema.
/// Returns a list of (child_table, parent_table) pairs.
pub async fn get_fk_dependencies(
    client: &Client,
    schema: &str,
) -> Result<Vec<FkDependency>> {
    let rows = client
        .query(
            "SELECT DISTINCT \
                 tc.table_name AS child_table, \
                 ccu.table_name AS parent_table \
             FROM information_schema.table_constraints tc \
             JOIN information_schema.constraint_column_usage ccu \
                 ON ccu.constraint_name = tc.constraint_name \
                 AND ccu.constraint_schema = tc.constraint_schema \
             WHERE tc.constraint_type = 'FOREIGN KEY' \
             AND tc.table_schema = $1 \
             AND ccu.table_schema = $1 \
             AND tc.table_name <> ccu.table_name",
            &[&schema],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| FkDependency {
            table: r.get(0),
            references_table: r.get(1),
        })
        .collect())
}

/// Sort tables for INSERT order: parents before children (topological sort), else in the order given.
pub fn sort_for_insert(tables: &[String], deps: &[FkDependency]) -> Vec<String> {
    topological_sort(tables, deps, false)
}

/// Sort tables for DELETE order: children before parents (reverse topological sort).
pub fn sort_for_delete(tables: &[String], deps: &[FkDependency]) -> Vec<String> {
    topological_sort(tables, deps, true)
}

/// Topological sort using Kahn's algorithm, stable: of the tables ready, the first given goes first.
/// Tables in a dependency cycle follow, in the order given. Names compare case-insensitively; a name
/// given twice is listed once. If `reverse` is true, returns delete order (children first).
fn topological_sort(tables: &[String], deps: &[FkDependency], reverse: bool) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    for t in tables {
        if !names.iter().any(|n| n.eq_ignore_ascii_case(t)) {
            names.push(t.clone());
        }
    }
    let index: HashMap<String, usize> = names.iter().enumerate().map(|(i, t)| (t.to_lowercase(), i)).collect();

    // parent -> children; a child's count of parents not yet placed
    let mut children: Vec<Vec<usize>> = vec![Vec::new(); names.len()];
    let mut in_degree = vec![0usize; names.len()];
    let mut edges: HashSet<(usize, usize)> = HashSet::new();
    for dep in deps {
        let (Some(&child), Some(&parent)) =
            (index.get(&dep.table.to_lowercase()), index.get(&dep.references_table.to_lowercase()))
        else {
            continue;
        };
        if child != parent && edges.insert((parent, child)) {
            children[parent].push(child);
            in_degree[child] += 1;
        }
    }

    let mut ready: BTreeSet<usize> = (0..names.len()).filter(|&i| in_degree[i] == 0).collect();
    let mut order: Vec<usize> = Vec::with_capacity(names.len());
    while let Some(i) = ready.pop_first() {
        order.push(i);
        for &c in &children[i] {
            in_degree[c] -= 1;
            if in_degree[c] == 0 {
                ready.insert(c);
            }
        }
    }
    // a cycle: its tables at the end, in the order given
    let rest: Vec<usize> = (0..names.len()).filter(|i| !order.contains(i)).collect();
    order.extend(rest);

    let mut ordered: Vec<String> = order.into_iter().map(|i| names[i].clone()).collect();
    if reverse {
        ordered.reverse();
    }
    ordered
}

/// Resolve which columns to use as PK: alternate keys if provided, else real PK.
/// Returns (pk_columns, real_pk_column_names).
pub fn resolve_pk_columns(
    columns: &[ColumnInfo],
    alternate_keys: Option<&[String]>,
) -> (Vec<ColumnInfo>, Vec<String>) {
    let real_pk: Vec<String> = columns
        .iter()
        .filter(|c| c.is_pk)
        .map(|c| c.name.clone())
        .collect();

    if let Some(alt_keys) = alternate_keys
        && !alt_keys.is_empty() {
            let pk_cols: Vec<ColumnInfo> = columns
                .iter()
                .filter(|c| {
                    alt_keys
                        .iter()
                        .any(|k| k.eq_ignore_ascii_case(&c.name))
                })
                .cloned()
                .collect();
            return (pk_cols, real_pk);
        }

    let pk_cols: Vec<ColumnInfo> = columns.iter().filter(|c| c.is_pk).cloned().collect();
    (pk_cols, real_pk)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, is_pk: bool) -> ColumnInfo {
        ColumnInfo { name: name.to_string(), pg_type: Type::TEXT, is_pk, is_identity: false, identity_always: false, ordinal: 0 }
    }

    fn dep(table: &str, references_table: &str) -> FkDependency {
        FkDependency { table: table.to_string(), references_table: references_table.to_string() }
    }

    #[test]
    fn a_type_name_resolves_to_its_postgres_type() {
        let cases = [
            ("bool", Type::BOOL), ("boolean", Type::BOOL),
            ("int2", Type::INT2), ("smallint", Type::INT2),
            ("int4", Type::INT4), ("integer", Type::INT4), ("int", Type::INT4), ("serial", Type::INT4),
            ("int8", Type::INT8), ("bigint", Type::INT8), ("bigserial", Type::INT8),
            ("float4", Type::FLOAT4), ("real", Type::FLOAT4),
            ("float8", Type::FLOAT8), ("double precision", Type::FLOAT8),
            ("numeric", Type::NUMERIC), ("decimal", Type::NUMERIC),
            ("text", Type::TEXT),
            ("varchar", Type::VARCHAR), ("character varying", Type::VARCHAR),
            ("char", Type::BPCHAR), ("character", Type::BPCHAR), ("bpchar", Type::BPCHAR),
            ("bytea", Type::BYTEA),
            ("timestamp", Type::TIMESTAMP), ("timestamp without time zone", Type::TIMESTAMP),
            ("timestamptz", Type::TIMESTAMPTZ), ("timestamp with time zone", Type::TIMESTAMPTZ),
            ("date", Type::DATE),
            ("time", Type::TIME), ("time without time zone", Type::TIME),
            ("uuid", Type::UUID), ("json", Type::JSON), ("jsonb", Type::JSONB),
            ("oid", Type::OID), ("name", Type::NAME),
            ("_int4", Type::TEXT), ("interval", Type::TEXT),
        ];
        for (name, expected) in cases {
            assert_eq!(resolve_pg_type(name), expected, "{}", name);
        }
    }

    #[test]
    fn parents_are_inserted_before_children_and_deleted_after() {
        let tables: Vec<String> = ["Orders", "customers", "order_items", "audit"].iter().map(|s| s.to_string()).collect();
        let deps = vec![
            dep("orders", "customers"),
            dep("order_items", "Orders"),
            // a dependency on a table outside the set is left out
            dep("audit", "elsewhere"),
        ];
        let insert = sort_for_insert(&tables, &deps);
        let pos = |v: &[String], t: &str| v.iter().position(|x| x == t).unwrap();
        assert_eq!(insert.len(), 4);
        assert!(pos(&insert, "customers") < pos(&insert, "Orders"));
        assert!(pos(&insert, "Orders") < pos(&insert, "order_items"));

        let delete = sort_for_delete(&tables, &deps);
        assert!(pos(&delete, "order_items") < pos(&delete, "Orders"));
        assert!(pos(&delete, "Orders") < pos(&delete, "customers"));
    }

    #[test]
    fn a_dependency_cycle_still_lists_every_table() {
        let tables: Vec<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
        let deps = vec![dep("a", "b"), dep("b", "a")];
        let sorted = sort_for_insert(&tables, &deps);
        assert_eq!(sorted.len(), 3);
        assert_eq!(sorted[0], "c");
        assert!(sorted.contains(&"a".to_string()) && sorted.contains(&"b".to_string()));
    }

    #[test]
    fn the_key_is_the_alternate_one_when_given_else_the_real_one() {
        let cols = vec![col("id", true), col("Code", false), col("name", false)];
        let (pk, real) = resolve_pk_columns(&cols, None);
        assert_eq!(pk.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["id"]);
        assert_eq!(real, vec!["id"]);

        let alt = vec!["code".to_string()];
        let (pk, real) = resolve_pk_columns(&cols, Some(&alt));
        assert_eq!(pk.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["Code"]);
        assert_eq!(real, vec!["id"]);

        let (pk, _) = resolve_pk_columns(&cols, Some(&[]));
        assert_eq!(pk.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["id"]);
    }
}

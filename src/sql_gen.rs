use std::collections::HashSet;
use std::fmt::Write as FmtWrite;

use postgres_types::Type;

use crate::types::{ColumnInfo, Row, RowValue};

/// Quote a PostgreSQL identifier (table/column name) if needed.
pub fn quote_identifier(name: &str) -> String {
    // Must quote if: contains uppercase, spaces, special chars, or is a reserved word.
    // For safety, always quote with double quotes.
    let needs_quoting = name.chars().any(|c| c.is_ascii_uppercase() || !c.is_ascii_alphanumeric() && c != '_')
        || name.is_empty()
        || name.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false);

    if needs_quoting {
        format!("\"{}\"", name.replace('"', "\"\""))
    } else {
        name.to_string()
    }
}

/// A column as the diff selects it: NUMERIC, and every column read as text - TEXT, which is also what a
/// type the diff does not decode maps to (an enum, an array, an interval, ...) - cast to text, so that its
/// value is read as its exact text form rather than lost.
pub fn select_column(col: &ColumnInfo) -> String {
    let name = quote_identifier(&col.name);
    if col.pg_type == Type::NUMERIC || col.pg_type == Type::TEXT {
        format!("{}::text", name)
    } else {
        name
    }
}

/// The select list of these columns, each as [`select_column`] selects it.
pub fn select_list(columns: &[ColumnInfo]) -> String {
    columns.iter().map(select_column).collect::<Vec<_>>().join(", ")
}

/// The key columns as text: what a row is matched by across two databases, whatever the key's types
/// (read back with [`crate::types::text_key`]). Each is named apart (`dd_key<i>`), so that an `ORDER BY`
/// of the key still names the key's own column.
pub fn pk_text_list(pk_cols: &[ColumnInfo]) -> String {
    pk_cols
        .iter()
        .enumerate()
        .map(|(i, c)| format!("{}::text AS dd_key{}", quote_identifier(&c.name), i))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Format a schema-qualified table name for use in SQL.
pub fn format_table_name(name: &str) -> String {
    if let Some(dot_pos) = name.find('.') {
        let schema = &name[..dot_pos];
        let table = &name[dot_pos + 1..];
        format!("{}.{}", quote_identifier(schema), quote_identifier(table))
    } else {
        quote_identifier(name)
    }
}

/// Format a `RowValue` as a PostgreSQL literal.
pub fn format_literal(value: &RowValue) -> String {
    match value {
        RowValue::Null => "NULL".to_string(),
        RowValue::Bool(b) => {
            if *b {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        RowValue::I16(v) => v.to_string(),
        RowValue::I32(v) => v.to_string(),
        RowValue::I64(v) => v.to_string(),
        RowValue::F32(v) => {
            if v.is_nan() {
                "'NaN'::real".to_string()
            } else if v.is_infinite() {
                if v.is_sign_positive() {
                    "'Infinity'::real".to_string()
                } else {
                    "'-Infinity'::real".to_string()
                }
            } else {
                v.to_string()
            }
        }
        RowValue::F64(v) => {
            if v.is_nan() {
                "'NaN'::double precision".to_string()
            } else if v.is_infinite() {
                if v.is_sign_positive() {
                    "'Infinity'::double precision".to_string()
                } else {
                    "'-Infinity'::double precision".to_string()
                }
            } else {
                v.to_string()
            }
        }
        // a number as it is; NaN and the infinities quoted, as numeric literals cannot spell them
        RowValue::Numeric(v) => {
            if !v.is_empty() && v.chars().all(|c| c.is_ascii_digit() || c == '-' || c == '.') {
                v.clone()
            } else {
                format!("{}::numeric", escape_string_literal(v))
            }
        }
        RowValue::Text(s) => escape_string_literal(s),
        RowValue::Bytes(b) => {
            let mut result = String::with_capacity(b.len() * 2 + 5);
            result.push_str("'\\x");
            for byte in b {
                write!(result, "{:02x}", byte).unwrap();
            }
            result.push('\'');
            result
        }
        RowValue::Timestamp(ts) => {
            format!("'{}'::timestamp", ts.format("%Y-%m-%d %H:%M:%S%.f"))
        }
        RowValue::TimestampTz(ts) => {
            format!("'{}'::timestamptz", ts.format("%Y-%m-%d %H:%M:%S%.f%:z"))
        }
        RowValue::Date(d) => {
            format!("'{}'::date", d.format("%Y-%m-%d"))
        }
        RowValue::Time(t) => {
            format!("'{}'::time", t.format("%H:%M:%S%.f"))
        }
        RowValue::Uuid(u) => {
            format!("'{}'::uuid", u)
        }
        RowValue::Json(v) => {
            let json_str = serde_json::to_string(v).unwrap_or_default();
            format!("{}::jsonb", escape_string_literal(&json_str))
        }
    }
}

/// Escape a string for use as a PostgreSQL string literal.
fn escape_string_literal(s: &str) -> String {
    if s.contains('\\') {
        // Use E'' syntax for strings with backslashes
        let escaped = s.replace('\'', "''").replace('\\', "\\\\");
        format!("E'{}'", escaped)
    } else {
        format!("'{}'", s.replace('\'', "''"))
    }
}

/// Generate an INSERT statement: `OVERRIDING SYSTEM VALUE` when it writes a `GENERATED ALWAYS` identity.
pub fn generate_insert(
    table: &str,
    columns: &[ColumnInfo],
    row: &[RowValue],
    exclude_cols: &HashSet<String>,
) -> String {
    let table_sql = format_table_name(table);

    let mut col_names = Vec::new();
    let mut values = Vec::new();
    let mut overriding = false;

    for (i, col) in columns.iter().enumerate() {
        if exclude_cols.contains(&col.name.to_lowercase()) {
            continue;
        }
        overriding |= col.identity_always;
        col_names.push(quote_identifier(&col.name));
        values.push(format_literal(&row[i]));
    }

    format!(
        "INSERT INTO {} ({}){} VALUES ({});",
        table_sql,
        col_names.join(", "),
        if overriding { " OVERRIDING SYSTEM VALUE" } else { "" },
        values.join(", "),
    )
}

/// Generate an UPDATE statement.
///
/// `modified_indices` are the column indices where reference differs from target.
pub fn generate_update(
    table: &str,
    columns: &[ColumnInfo],
    pk_cols: &[ColumnInfo],
    row: &[RowValue],
    modified_indices: &[usize],
) -> Option<String> {
    if modified_indices.is_empty() {
        return None;
    }

    let table_sql = format_table_name(table);

    let set_clauses: Vec<String> = modified_indices
        .iter()
        .map(|&i| {
            format!(
                "{} = {}",
                quote_identifier(&columns[i].name),
                format_literal(&row[i])
            )
        })
        .collect();

    let where_clauses: Vec<String> = pk_cols
        .iter()
        .map(|pk| {
            let idx = columns.iter().position(|c| c.name.eq_ignore_ascii_case(&pk.name)).unwrap();
            let val = &row[idx];
            if matches!(val, RowValue::Null) {
                format!("{} IS NULL", quote_identifier(&pk.name))
            } else {
                format!("{} = {}", quote_identifier(&pk.name), format_literal(val))
            }
        })
        .collect();

    Some(format!(
        "UPDATE {} SET {} WHERE {};",
        table_sql,
        set_clauses.join(", "),
        where_clauses.join(" AND "),
    ))
}

/// Generate a DELETE statement.
pub fn generate_delete(
    table: &str,
    columns: &[ColumnInfo],
    pk_cols: &[ColumnInfo],
    row: &[RowValue],
) -> String {
    let table_sql = format_table_name(table);

    let where_clauses: Vec<String> = pk_cols
        .iter()
        .map(|pk| {
            let idx = columns.iter().position(|c| c.name.eq_ignore_ascii_case(&pk.name)).unwrap();
            let val = &row[idx];
            if matches!(val, RowValue::Null) {
                format!("{} IS NULL", quote_identifier(&pk.name))
            } else {
                format!("{} = {}", quote_identifier(&pk.name), format_literal(val))
            }
        })
        .collect();

    format!("DELETE FROM {} WHERE {};", table_sql, where_clauses.join(" AND "))
}

/// Build a SELECT query to retrieve matching rows from a target table,
/// given a chunk of reference rows and their PK values.
///
/// Produces: `SELECT col1, col2, ... FROM target WHERE (pk1 = v1 AND pk2 = v2) OR ...`
pub fn build_check_query(
    table: &str,
    columns: &[ColumnInfo],
    pk_cols: &[ColumnInfo],
    rows: &[Row],
) -> String {
    let table_sql = format_table_name(table);

    let mut sql = format!("SELECT {} FROM {} WHERE ", select_list(columns), table_sql);

    for (row_idx, row) in rows.iter().enumerate() {
        if row_idx > 0 {
            sql.push_str(" OR ");
        }
        sql.push('(');
        for (pk_idx, pk) in pk_cols.iter().enumerate() {
            if pk_idx > 0 {
                sql.push_str(" AND ");
            }
            let col_idx = columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(&pk.name))
                .unwrap();
            let val = &row[col_idx];
            sql.push_str(&quote_identifier(&pk.name));
            if matches!(val, RowValue::Null) {
                sql.push_str(" IS NULL");
            } else {
                sql.push_str(" = ");
                sql.push_str(&format_literal(val));
            }
        }
        sql.push(')');
    }

    sql
}

/// Build a SELECT query for PK-only columns from a table (used by delete sync).
pub fn build_pk_select(table: &str, pk_cols: &[ColumnInfo]) -> String {
    format!("SELECT {} FROM {}", select_list(pk_cols), format_table_name(table))
}

/// Build a check query for the delete sync: checks which PK values exist in the reference table.
pub fn build_delete_check_query(
    reference_table: &str,
    pk_cols: &[ColumnInfo],
    rows: &[Row],
) -> String {
    let table_sql = format_table_name(reference_table);

    let mut sql = format!("SELECT {} FROM {} WHERE ", select_list(pk_cols), table_sql);

    for (row_idx, row) in rows.iter().enumerate() {
        if row_idx > 0 {
            sql.push_str(" OR ");
        }
        sql.push('(');
        for (pk_idx, pk) in pk_cols.iter().enumerate() {
            if pk_idx > 0 {
                sql.push_str(" AND ");
            }
            let val = &row[pk_idx]; // rows only contain PK values in order
            sql.push_str(&quote_identifier(&pk.name));
            if matches!(val, RowValue::Null) {
                sql.push_str(" IS NULL");
            } else {
                sql.push_str(" = ");
                sql.push_str(&format_literal(val));
            }
        }
        sql.push(')');
    }

    sql
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, is_pk: bool) -> ColumnInfo {
        ColumnInfo { name: name.to_string(), pg_type: Type::INT4, is_pk, is_identity: false, identity_always: false, ordinal: 0 }
    }

    fn typed(name: &str, pg_type: Type) -> ColumnInfo {
        ColumnInfo { name: name.to_string(), pg_type, is_pk: false, is_identity: false, identity_always: false, ordinal: 0 }
    }

    #[test]
    fn numeric_and_text_read_columns_are_selected_as_text_and_keys_always() {
        let cols = vec![
            typed("id", Type::INT4),
            typed("Amount", Type::NUMERIC),
            typed("mood", Type::TEXT),
            typed("name", Type::VARCHAR),
            typed("at", Type::TIMESTAMP),
        ];
        assert_eq!(select_list(&cols), "id, \"Amount\"::text, mood::text, name, at");
        assert_eq!(pk_text_list(&cols[..2]), "id::text AS dd_key0, \"Amount\"::text AS dd_key1");
        assert_eq!(build_pk_select("t", &cols[1..3]), "SELECT \"Amount\"::text, mood::text FROM t");
    }

    #[test]
    fn an_identifier_is_quoted_only_when_it_must_be() {
        assert_eq!(quote_identifier("users"), "users");
        assert_eq!(quote_identifier("user_id2"), "user_id2");
        assert_eq!(quote_identifier("Users"), "\"Users\"");
        assert_eq!(quote_identifier("my col"), "\"my col\"");
        assert_eq!(quote_identifier("1st"), "\"1st\"");
        assert_eq!(quote_identifier(""), "\"\"");
        assert_eq!(quote_identifier("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn a_table_name_quotes_its_schema_and_table_apart() {
        assert_eq!(format_table_name("users"), "users");
        assert_eq!(format_table_name("public.users"), "public.users");
        assert_eq!(format_table_name("My.Table"), "\"My\".\"Table\"");
    }

    #[test]
    fn every_value_renders_as_a_postgres_literal() {
        let ts = chrono::NaiveDate::from_ymd_opt(2024, 2, 29).unwrap().and_hms_micro_opt(13, 4, 5, 250_000).unwrap();
        let cases = vec![
            (RowValue::Null, "NULL".to_string()),
            (RowValue::Bool(true), "TRUE".to_string()),
            (RowValue::Bool(false), "FALSE".to_string()),
            (RowValue::I16(-7), "-7".to_string()),
            (RowValue::I32(42), "42".to_string()),
            (RowValue::I64(9_000_000_000), "9000000000".to_string()),
            (RowValue::F32(1.5), "1.5".to_string()),
            (RowValue::F32(f32::NAN), "'NaN'::real".to_string()),
            (RowValue::F32(f32::INFINITY), "'Infinity'::real".to_string()),
            (RowValue::F32(f32::NEG_INFINITY), "'-Infinity'::real".to_string()),
            (RowValue::F64(2.25), "2.25".to_string()),
            (RowValue::F64(f64::NAN), "'NaN'::double precision".to_string()),
            (RowValue::F64(f64::INFINITY), "'Infinity'::double precision".to_string()),
            (RowValue::F64(f64::NEG_INFINITY), "'-Infinity'::double precision".to_string()),
            (RowValue::Numeric("-123.45".into()), "-123.45".to_string()),
            (RowValue::Numeric("NaN".into()), "'NaN'::numeric".to_string()),
            (RowValue::Numeric("-Infinity".into()), "'-Infinity'::numeric".to_string()),
            (RowValue::Numeric(String::new()), "''::numeric".to_string()),
            (RowValue::Text("it's".into()), "'it''s'".to_string()),
            (RowValue::Text("a\\b'c".into()), "E'a\\\\b''c'".to_string()),
            (RowValue::Bytes(vec![0x00, 0xab, 0x10]), "'\\x00ab10'".to_string()),
            (RowValue::Timestamp(ts), "'2024-02-29 13:04:05.250'::timestamp".to_string()),
            (RowValue::TimestampTz(ts.and_utc()), "'2024-02-29 13:04:05.250+00:00'::timestamptz".to_string()),
            (RowValue::Date(ts.date()), "'2024-02-29'::date".to_string()),
            (RowValue::Time(ts.time()), "'13:04:05.250'::time".to_string()),
            (
                RowValue::Uuid(uuid::Uuid::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef)),
                "'01234567-89ab-cdef-0123-456789abcdef'::uuid".to_string(),
            ),
            (RowValue::Json(serde_json::json!({"k": "it's"})), "'{\"k\":\"it''s\"}'::jsonb".to_string()),
        ];
        for (value, expected) in cases {
            assert_eq!(format_literal(&value), expected, "{:?}", value);
        }
    }

    #[test]
    fn an_insert_leaves_out_the_excluded_columns() {
        let cols = vec![col("id", true), col("Name", false), col("secret", false)];
        let row = vec![RowValue::I32(1), RowValue::Text("x".into()), RowValue::Text("s".into())];
        let exclude: HashSet<String> = ["secret".to_string()].into();
        assert_eq!(
            generate_insert("public.users", &cols, &row, &exclude),
            "INSERT INTO public.users (id, \"Name\") VALUES (1, 'x');"
        );
        assert_eq!(
            generate_insert("users", &cols, &row, &HashSet::new()),
            "INSERT INTO users (id, \"Name\", secret) VALUES (1, 'x', 's');"
        );
    }

    #[test]
    fn an_update_sets_the_modified_columns_by_key() {
        let cols = vec![col("id", true), col("tenant", true), col("name", false), col("note", false)];
        let row = vec![RowValue::I32(1), RowValue::Null, RowValue::Text("n".into()), RowValue::Null];
        let pk = vec![col("id", true), col("TENANT", true)];
        assert_eq!(generate_update("t", &cols, &pk, &row, &[]), None);
        assert_eq!(
            generate_update("t", &cols, &pk, &row, &[2, 3]).unwrap(),
            "UPDATE t SET name = 'n', note = NULL WHERE id = 1 AND \"TENANT\" IS NULL;"
        );
    }

    #[test]
    fn a_delete_is_by_key() {
        let cols = vec![col("id", true), col("tenant", true)];
        let row = vec![RowValue::I64(5), RowValue::Null];
        assert_eq!(generate_delete("s.t", &cols, &cols, &row), "DELETE FROM s.t WHERE id = 5 AND tenant IS NULL;");
    }

    #[test]
    fn the_check_queries_or_together_one_key_per_row() {
        let cols = vec![col("id", true), col("tenant", true), col("name", false)];
        let pk = vec![col("id", true), col("tenant", true)];
        let rows = vec![
            vec![RowValue::I32(1), RowValue::Text("a".into()), RowValue::Text("x".into())],
            vec![RowValue::I32(2), RowValue::Null, RowValue::Null],
        ];
        assert_eq!(
            build_check_query("t", &cols, &pk, &rows),
            "SELECT id, tenant, name FROM t WHERE (id = 1 AND tenant = 'a') OR (id = 2 AND tenant IS NULL)"
        );
        assert_eq!(build_pk_select("public.t", &pk), "SELECT id, tenant FROM public.t");
        let pk_rows = vec![vec![RowValue::I32(1), RowValue::Text("a".into())], vec![RowValue::I32(2), RowValue::Null]];
        assert_eq!(
            build_delete_check_query("t", &pk, &pk_rows),
            "SELECT id, tenant FROM t WHERE (id = 1 AND tenant = 'a') OR (id = 2 AND tenant IS NULL)"
        );
    }
}

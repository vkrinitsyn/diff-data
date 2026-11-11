//! The `diff_data` binary as a user runs it: a bad connection string refused with exit code 1, and - with
//! `DIFF_DATA_PG` set, as for `tests/diff_live.rs` - a whole diff of two databases it creates and drops
//! (`dd_cli_ref`, `dd_cli_tgt`), its progress and its report on stderr.

use std::process::Command;

use tokio_postgres::{Client, NoTls};

fn bin() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_diff_data"));
    c.env("DIFF_DATA_LOCALE", "en");
    c
}

async fn connect(url: &str) -> Client {
    let (c, conn) = tokio_postgres::connect(url, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    c
}

#[test]
fn a_bad_connection_string_exits_with_an_error() {
    let out = bin().args(["-r", "port=notaport", "-t", "host=x", "-o", "master.sql"]).output().unwrap();
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.starts_with("DA-E018 the reference database: DA-E009 invalid connection string: "), "{}", stderr);
    // in the language asked for
    let ru = bin().env("DIFF_DATA_LOCALE", "ru").args(["-r", "port=notaport", "-t", "host=x", "-o", "m.sql"]).output().unwrap();
    assert!(String::from_utf8_lossy(&ru.stderr).starts_with("DA-E018 эталонная база: DA-E009 неверная строка подключения: "));
}

/// --version: the crate's, which the packages that ship this binary (sge's, YT Cluster's) check
/// (0.4.1, 2026-11-11)
#[test]
fn the_version_is_the_crates() {
    let out = bin().arg("--version").output().unwrap();
    assert!(out.status.success(), "{:?}", out);
    assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), format!("data-diff {}", env!("CARGO_PKG_VERSION")));
}

#[test]
fn the_help_lists_every_option() {
    let out = bin().arg("--help").output().unwrap();
    assert!(out.status.success());
    let help = String::from_utf8_lossy(&out.stdout);
    assert!(help.contains("--reference-url") && help.contains("--checksum-only"));
    assert!(help.contains("use sge") && !help.contains("cli."), "{}", help);
}

#[tokio::test]
async fn a_diff_runs_from_the_command_line() {
    let Ok(base) = std::env::var("DIFF_DATA_PG") else { return };
    let admin = connect(&format!("{} dbname=postgres", base)).await;
    for db in ["dd_cli_ref", "dd_cli_tgt"] {
        admin.batch_execute(&format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)")).await.unwrap();
        admin.batch_execute(&format!("CREATE DATABASE {db}")).await.unwrap();
    }
    let (r_url, t_url) = (format!("{} dbname=dd_cli_ref", base), format!("{} dbname=dd_cli_tgt", base));
    connect(&r_url).await.batch_execute("CREATE TABLE users (id int PRIMARY KEY, name text, seen int); \
        INSERT INTO users VALUES (1, 'a', 1), (2, 'b', 1), (3, 'c', 1)").await.unwrap();
    connect(&t_url).await.batch_execute("CREATE TABLE users (id int PRIMARY KEY, name text, seen int); \
        INSERT INTO users VALUES (1, 'a', 9), (2, 'B', 1), (4, 'd', 1)").await.unwrap();
    let out = std::env::temp_dir().join(format!("diff_data_cli_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&out);
    let master = out.join("master.sql");

    let run = bin()
        .args(["-r", &r_url, "-t", &t_url, "-o", master.to_str().unwrap(), "--table", "users", "--include-delete",
            "--chunk-size", "2", "--ignore-column", "seen", "--exclude-ignored-columns", "--alternate-key", "users:id",
            "--reference-db-name", "dd_cli_ref", "--target-db-name", "dd_cli_tgt",
            "--reference-schema", "public", "--target-schema", "public", "--exclude-identity", "--locale", "en"])
        .output().unwrap();
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(run.status.success(), "{}", stderr);
    assert!(stderr.lines().count() > 5, "progress and the report: {}", stderr);
    let sql = std::fs::read_to_string(out.join("users_$insert.sql")).unwrap();
    assert!(sql.contains("INSERT INTO users (id, name) VALUES (3, 'c');"), "{}", sql);
    assert!(std::fs::read_to_string(out.join("users_$delete.sql")).unwrap().contains("DELETE FROM users WHERE id = 4;"));
    assert!(std::fs::read_to_string(&master).unwrap().contains("\\i 'users_$update.sql'"));

    // quiet, its files elsewhere, identities written
    let elsewhere = out.join("elsewhere");
    let run = bin()
        .args(["-r", &r_url, "-t", &t_url, "-o", master.to_str().unwrap(), "--output-dir", elsewhere.to_str().unwrap(),
            "--table", "users:users", "--no-progress", "--include-identity", "--force"])
        .output().unwrap();
    assert!(run.status.success(), "{}", String::from_utf8_lossy(&run.stderr));
    assert!(elsewhere.join("users_$update.sql").exists());

    // a diff that fails: exit code 1
    let run = bin().args(["-r", &r_url, "-t", &t_url, "-o", master.to_str().unwrap(), "--table", "users",
        "--max-etc-seconds", "0", "--no-progress"]).output().unwrap();
    assert_eq!(run.status.code(), Some(1));

    let _ = std::fs::remove_dir_all(&out);
    for db in ["dd_cli_ref", "dd_cli_tgt"] {
        admin.batch_execute(&format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)")).await.unwrap();
    }
}

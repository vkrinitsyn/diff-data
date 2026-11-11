#![forbid(unsafe_code)]

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use clap::{ArgAction, Command, CommandFactory, FromArgMatches, Parser};
use rust_i18n::t;

rust_i18n::i18n!("locales", fallback = "en");

/// CLI struct — help text is injected dynamically via `apply_i18n()`.
#[derive(Parser, Debug)]
#[command(name = "data-diff")]
struct Cli {
    #[arg(short = 'r', long = "reference-url")]
    reference_url: String,

    #[arg(short = 't', long = "target-url")]
    target_url: String,

    #[arg(short = 'o', long)]
    output: PathBuf,

    #[arg(long = "output-dir")]
    output_dir: Option<PathBuf>,

    #[arg(long, action = ArgAction::Append)]
    table: Vec<String>,

    #[arg(long)]
    include_delete: bool,

    #[arg(long, default_value_t = 25)]
    chunk_size: usize,

    #[arg(long = "ignore-column", action = ArgAction::Append)]
    ignore_column: Vec<String>,

    #[arg(long = "alternate-key", action = ArgAction::Append)]
    alternate_key: Vec<String>,

    #[arg(long)]
    exclude_real_pk: bool,

    #[arg(long)]
    exclude_ignored_columns: bool,

    #[arg(long = "where")]
    where_condition: Option<String>,

    #[arg(long)]
    single_file: bool,

    #[arg(long)]
    reference_schema: Option<String>,

    #[arg(long)]
    target_schema: Option<String>,

    #[arg(long = "exclude-table", action = ArgAction::Append)]
    exclude_table: Vec<String>,

    #[arg(long)]
    no_check_dependencies: bool,

    #[arg(long, conflicts_with = "exclude_identity")]
    include_identity: bool,

    #[arg(long, conflicts_with = "include_identity")]
    exclude_identity: bool,

    #[arg(long)]
    timestamp_column: Option<String>,

    #[arg(long)]
    force: bool,

    #[arg(long)]
    no_progress: bool,

    #[arg(long)]
    reference_db_name: Option<String>,

    #[arg(long)]
    target_db_name: Option<String>,

    #[arg(long, default_value_t = 10800.0)]
    max_etc_seconds: f64,

    #[arg(long)]
    reference_checksum_file: Option<PathBuf>,

    #[arg(long)]
    target_checksum_file: Option<PathBuf>,

    #[arg(long)]
    save_reference_checksums: Option<PathBuf>,

    #[arg(long)]
    save_target_checksums: Option<PathBuf>,

    #[arg(long)]
    checksum_only: bool,

    #[arg(long)]
    locale: Option<String>,
}

/// Detect locale from environment or CLI, set it for rust-i18n.
fn detect_and_set_locale() {
    let locale = std::env::var("DIFF_DATA_LOCALE")
        .or_else(|_| {
            std::env::var("LANG").map(|l| {
                // LANG is often "en_US.UTF-8"; take just the language part
                l.split('.').next().unwrap_or("en").split('_').next().unwrap_or("en").to_string()
            })
        })
        .unwrap_or_else(|_| "en".to_string());
    rust_i18n::set_locale(&locale);
}

/// Build a `Command` with i18n help text injected into each argument.
fn apply_i18n() -> Command {
    let cmd = Cli::command();
    cmd.about(t!("cli.about").to_string())
        .mut_arg("reference_url", |a| a.help(t!("cli.reference_url").to_string()))
        .mut_arg("target_url", |a| a.help(t!("cli.target_url").to_string()))
        .mut_arg("output", |a| a.help(t!("cli.output").to_string()))
        .mut_arg("output_dir", |a| a.help(t!("cli.output_dir").to_string()))
        .mut_arg("table", |a| a.help(t!("cli.table").to_string()))
        .mut_arg("include_delete", |a| a.help(t!("cli.include_delete").to_string()))
        .mut_arg("chunk_size", |a| a.help(t!("cli.chunk_size").to_string()))
        .mut_arg("ignore_column", |a| a.help(t!("cli.ignore_column").to_string()))
        .mut_arg("alternate_key", |a| a.help(t!("cli.alternate_key").to_string()))
        .mut_arg("exclude_real_pk", |a| a.help(t!("cli.exclude_real_pk").to_string()))
        .mut_arg("exclude_ignored_columns", |a| a.help(t!("cli.exclude_ignored_columns").to_string()))
        .mut_arg("where_condition", |a| a.help(t!("cli.where_condition").to_string()))
        .mut_arg("single_file", |a| a.help(t!("cli.single_file").to_string()))
        .mut_arg("reference_schema", |a| a.help(t!("cli.reference_schema").to_string()))
        .mut_arg("target_schema", |a| a.help(t!("cli.target_schema").to_string()))
        .mut_arg("exclude_table", |a| a.help(t!("cli.exclude_table").to_string()))
        .mut_arg("no_check_dependencies", |a| a.help(t!("cli.no_check_dependencies").to_string()))
        .mut_arg("include_identity", |a| a.help(t!("cli.include_identity").to_string()))
        .mut_arg("exclude_identity", |a| a.help(t!("cli.exclude_identity").to_string()))
        .mut_arg("timestamp_column", |a| a.help(t!("cli.timestamp_column").to_string()))
        .mut_arg("force", |a| a.help(t!("cli.force").to_string()))
        .mut_arg("no_progress", |a| a.help(t!("cli.no_progress").to_string()))
        .mut_arg("reference_db_name", |a| a.help(t!("cli.reference_db_name").to_string()))
        .mut_arg("target_db_name", |a| a.help(t!("cli.target_db_name").to_string()))
        .mut_arg("max_etc_seconds", |a| a.help(t!("cli.max_etc_seconds").to_string()))
        .mut_arg("reference_checksum_file", |a| a.help(t!("cli.reference_checksum_file").to_string()))
        .mut_arg("target_checksum_file", |a| a.help(t!("cli.target_checksum_file").to_string()))
        .mut_arg("save_reference_checksums", |a| a.help(t!("cli.save_reference_checksums").to_string()))
        .mut_arg("save_target_checksums", |a| a.help(t!("cli.save_target_checksums").to_string()))
        .mut_arg("checksum_only", |a| a.help(t!("cli.checksum_only").to_string()))
        .mut_arg("locale", |a| a.help(t!("cli.locale").to_string()))
}

/// Parse table pair strings ("ref:target" or "table") into `TablePair` values.
fn parse_table_pairs(raw: &[String]) -> Result<Vec<diff_data::TablePair>, String> {
    raw.iter()
        .map(|s| {
            if let Some((r, t)) = s.split_once(':') {
                Ok(diff_data::TablePair {
                    reference: r.to_string(),
                    target: t.to_string(),
                })
            } else {
                Ok(diff_data::TablePair {
                    reference: s.clone(),
                    target: s.clone(),
                })
            }
        })
        .collect()
}

/// Parse alternate key strings ("table:col1,col2") into a HashMap.
fn parse_alternate_keys(raw: &[String]) -> Result<HashMap<String, Vec<String>>, String> {
    let mut map = HashMap::new();
    for s in raw {
        let (table, cols_str) = s.split_once(':').ok_or_else(|| {
            t!("error.invalid_alternate_key", value = s).to_string()
        })?;
        let cols: Vec<String> = cols_str.split(',').map(|c| c.trim().to_string()).collect();
        map.insert(table.to_string(), cols);
    }
    Ok(map)
}

/// Format and print a progress event to stderr.
fn print_progress(event: &diff_data::config::ProgressEvent) {
    use diff_data::config::ProgressEvent::*;
    let msg = match event {
        TableStart { table, table_index, total_tables } => {
            t!("progress.table_start",
                table = table,
                index = table_index + 1,
                total = total_tables
            ).to_string()
        }
        RowProcessed { table, row_number } => {
            if row_number % 1000 != 0 {
                return;
            }
            t!("progress.row_processed", table = table, count = row_number).to_string()
        }
        TableDiffComplete { table, inserts, updates } => {
            t!("progress.table_diff_complete",
                table = table,
                inserts = inserts,
                updates = updates
            ).to_string()
        }
        TableDeleteComplete { table, deletes } => {
            t!("progress.table_delete_complete", table = table, deletes = deletes).to_string()
        }
        SortingDependencies => {
            t!("progress.sorting_dependencies").to_string()
        }
        SchemaCompareStart => {
            t!("progress.schema_compare_start").to_string()
        }
        SchemaCompareResult { table, result } => {
            t!("progress.schema_compare_result", table = table, result = result).to_string()
        }
        EstimationStart { total_tables } => {
            t!("progress.estimation_start", total = total_tables).to_string()
        }
        EstimationTableDone { table, estimated_seconds, strategy } => {
            t!("progress.estimation_table_done",
                table = table,
                seconds = format!("{:.1}", estimated_seconds),
                strategy = strategy
            ).to_string()
        }
        EstimationComplete { total_estimated_seconds } => {
            t!("progress.estimation_complete",
                seconds = format!("{:.1}", total_estimated_seconds)
            ).to_string()
        }
        ChunkProgress { table, chunk_index, total_chunks } => {
            t!("progress.chunk_progress",
                table = table,
                index = chunk_index + 1,
                total = total_chunks
            ).to_string()
        }
    };
    eprintln!("{}", msg);
}

/// Print the diff report to stderr.
fn print_report(report: &diff_data::DiffReport) {
    eprintln!("{}", t!("report.header"));

    if report.tables.is_empty() {
        eprintln!("{}", t!("report.no_differences"));
    } else {
        for tr in &report.tables {
            eprintln!(
                "{}",
                t!("report.table_line",
                    table = &tr.table_name,
                    inserts = tr.inserts,
                    updates = tr.updates,
                    deletes = tr.deletes
                )
            );
        }
    }

    if let Some(ref path) = report.master_script {
        eprintln!("{}", t!("report.master_script", path = path));
    }

    if !report.warnings.is_empty() {
        eprintln!("{}", t!("report.warnings_header"));
        for w in &report.warnings {
            eprintln!("{}", t!("report.warning_line", message = w));
        }
    }
}

/// Core async logic, separated from main for clean error handling.
async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    // Override locale if --locale was passed
    if let Some(ref locale) = cli.locale {
        rust_i18n::set_locale(locale);
    }

    // Create connection pools
    let reference_pool = diff_data::create_pool(&cli.reference_url).await.map_err(|e| {
        t!("error.connection_failed", which = "reference", error = e.to_string()).to_string()
    })?;
    let target_pool = diff_data::create_pool(&cli.target_url).await.map_err(|e| {
        t!("error.connection_failed", which = "target", error = e.to_string()).to_string()
    })?;

    // Parse complex CLI values
    let tables = parse_table_pairs(&cli.table).map_err(|e| e)?;
    let alternate_keys = parse_alternate_keys(&cli.alternate_key).map_err(|e| e)?;

    let include_identity_columns = if cli.include_identity {
        Some(true)
    } else if cli.exclude_identity {
        Some(false)
    } else {
        None
    };

    let output_dir = cli.output_dir.clone().unwrap_or_else(|| {
        cli.output
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."))
    });

    let show_progress = !cli.no_progress;

    // Build the progress callback
    let on_progress: Option<Arc<dyn Fn(diff_data::config::ProgressEvent) + Send + Sync>> =
        if show_progress {
            Some(Arc::new(|event| print_progress(&event)))
        } else {
            None
        };

    let config = diff_data::DiffConfig {
        reference_pool,
        target_pool,
        tables,
        output_dir,
        output_file: cli.output,
        include_delete: cli.include_delete,
        chunk_size: cli.chunk_size,
        columns_to_ignore: cli.ignore_column.into_iter().collect::<HashSet<String>>(),
        alternate_keys,
        exclude_real_pk: cli.exclude_real_pk,
        exclude_ignored_columns: cli.exclude_ignored_columns,
        table_where: cli.where_condition,
        single_file: cli.single_file,
        reference_schema: cli.reference_schema,
        target_schema: cli.target_schema,
        exclude_tables: cli.exclude_table.into_iter().collect::<HashSet<String>>(),
        check_dependencies: !cli.no_check_dependencies,
        include_identity_columns,
        timestamp_column: cli.timestamp_column,
        force: cli.force,
        show_progress,
        reference_db_name: cli.reference_db_name,
        target_db_name: cli.target_db_name,
        max_etc_seconds: cli.max_etc_seconds,
        output_stream: None,
        on_progress,
        reference_checksum_file: cli.reference_checksum_file,
        target_checksum_file: cli.target_checksum_file,
        save_reference_checksums: cli.save_reference_checksums,
        save_target_checksums: cli.save_target_checksums,
        checksum_only: cli.checksum_only,
    };

    let report = diff_data::run_diff(config).await?;
    print_report(&report);

    Ok(())
}

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();

    // Detect locale before CLI parsing so --help renders in the correct language
    detect_and_set_locale();

    // Parse CLI with i18n help text
    let matches = apply_i18n().get_matches();
    let cli = Cli::from_arg_matches(&matches).expect("failed to parse CLI arguments");

    if let Err(e) = run(cli).await {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

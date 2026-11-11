#![forbid(unsafe_code)]

//! diff_data's command line: the data diff of two PostgreSQL databases. The data only - a comprehensive
//! diff, schema and data, is sge's (Schema Guard Enterprise). Its texts are diff_data's ([diff_data::i18n]),
//! in the language of --locale, else DIFF_DATA_LOCALE, else LANG.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use clap::{ArgAction, Command, CommandFactory, FromArgMatches, Parser};
use diff_data::i18n::message;

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

/// The language: DIFF_DATA_LOCALE when set, else diff_data's own default (LANG)
fn detect_and_set_locale() {
    if let Ok(locale) = std::env::var("DIFF_DATA_LOCALE") {
        diff_data::i18n::set_lang(&locale);
    }
}

/// A text of the command line, no placeholder
fn text(key: &str) -> String {
    message(key, &[])
}

/// Build a `Command` with i18n help text injected into each argument.
fn apply_i18n() -> Command {
    let cmd = Cli::command();
    let args = [
        "reference_url", "target_url", "output", "output_dir", "table", "include_delete", "chunk_size",
        "ignore_column", "alternate_key", "exclude_real_pk", "exclude_ignored_columns", "where_condition",
        "single_file", "reference_schema", "target_schema", "exclude_table", "no_check_dependencies",
        "include_identity", "exclude_identity", "timestamp_column", "force", "no_progress",
        "reference_db_name", "target_db_name", "max_etc_seconds", "reference_checksum_file",
        "target_checksum_file", "save_reference_checksums", "save_target_checksums", "checksum_only", "locale",
    ];
    args.iter().fold(cmd.about(text("cli.about")), |cmd, arg| {
        cmd.mut_arg(*arg, |a| a.help(help_of(arg)))
    })
}

/// An option's help, by its name: cli.<name>
fn help_of(arg: &str) -> String {
    // spelled out, so that the key test of diff_data's i18n finds each key asked for
    let key = match arg {
        "reference_url" => "cli.reference_url",
        "target_url" => "cli.target_url",
        "output" => "cli.output",
        "output_dir" => "cli.output_dir",
        "table" => "cli.table",
        "include_delete" => "cli.include_delete",
        "chunk_size" => "cli.chunk_size",
        "ignore_column" => "cli.ignore_column",
        "alternate_key" => "cli.alternate_key",
        "exclude_real_pk" => "cli.exclude_real_pk",
        "exclude_ignored_columns" => "cli.exclude_ignored_columns",
        "where_condition" => "cli.where_condition",
        "single_file" => "cli.single_file",
        "reference_schema" => "cli.reference_schema",
        "target_schema" => "cli.target_schema",
        "exclude_table" => "cli.exclude_table",
        "no_check_dependencies" => "cli.no_check_dependencies",
        "include_identity" => "cli.include_identity",
        "exclude_identity" => "cli.exclude_identity",
        "timestamp_column" => "cli.timestamp_column",
        "force" => "cli.force",
        "no_progress" => "cli.no_progress",
        "reference_db_name" => "cli.reference_db_name",
        "target_db_name" => "cli.target_db_name",
        "max_etc_seconds" => "cli.max_etc_seconds",
        "reference_checksum_file" => "cli.reference_checksum_file",
        "target_checksum_file" => "cli.target_checksum_file",
        "save_reference_checksums" => "cli.save_reference_checksums",
        "save_target_checksums" => "cli.save_target_checksums",
        "checksum_only" => "cli.checksum_only",
        _ => "cli.locale",
    };
    text(key)
}

/// Parse table pair strings ("ref:target" or "table") into `TablePair` values.
fn parse_table_pairs(raw: &[String]) -> Vec<diff_data::TablePair> {
    raw.iter()
        .map(|s| match s.split_once(':') {
            Some((r, t)) => diff_data::TablePair { reference: r.to_string(), target: t.to_string() },
            None => diff_data::TablePair { reference: s.clone(), target: s.clone() },
        })
        .collect()
}

/// Parse alternate key strings ("table:col1,col2") into a HashMap.
fn parse_alternate_keys(raw: &[String]) -> Result<HashMap<String, Vec<String>>, String> {
    let mut map = HashMap::new();
    for s in raw {
        let (table, cols_str) = s.split_once(':').ok_or_else(|| message("error.alternate_key", &[("value", s)]))?;
        let cols: Vec<String> = cols_str.split(',').map(|c| c.trim().to_string()).collect();
        map.insert(table.to_string(), cols);
    }
    Ok(map)
}

/// The line a progress event prints on stderr; None for a row count between thousands
fn progress_line(event: &diff_data::config::ProgressEvent) -> Option<String> {
    use diff_data::config::ProgressEvent::*;
    Some(match event {
        TableStart { table, table_index, total_tables } => message(
            "progress.table_start",
            &[("table", table), ("index", &(table_index + 1).to_string()), ("total", &total_tables.to_string())],
        ),
        RowProcessed { table, row_number } => {
            if row_number % 1000 != 0 {
                return None;
            }
            message("progress.row_processed", &[("table", table), ("count", &row_number.to_string())])
        }
        TableDiffComplete { table, inserts, updates } => message(
            "progress.table_diff_complete",
            &[("table", table), ("inserts", &inserts.to_string()), ("updates", &updates.to_string())],
        ),
        TableDeleteComplete { table, deletes } => {
            message("progress.table_delete_complete", &[("table", table), ("deletes", &deletes.to_string())])
        }
        SortingDependencies => text("progress.sorting_dependencies"),
        EstimationStart { total_tables } => message("progress.estimation_start", &[("total", &total_tables.to_string())]),
        EstimationTableDone { table, estimated_seconds, strategy } => message(
            "progress.estimation_table_done",
            &[("table", table), ("seconds", &format!("{:.1}", estimated_seconds)), ("strategy", strategy)],
        ),
        EstimationComplete { total_estimated_seconds } => {
            message("progress.estimation_complete", &[("seconds", &format!("{:.1}", total_estimated_seconds))])
        }
        ChunkProgress { table, chunk_index, total_chunks } => message(
            "progress.chunk_progress",
            &[("table", table), ("index", &(chunk_index + 1).to_string()), ("total", &total_chunks.to_string())],
        ),
    })
}

/// Format and print a progress event to stderr.
fn print_progress(event: &diff_data::config::ProgressEvent) {
    if let Some(line) = progress_line(event) {
        eprintln!("{}", line);
    }
}

/// The report's lines
fn report_lines(report: &diff_data::DiffReport) -> Vec<String> {
    let mut lines = vec![text("report.header")];
    if report.tables.is_empty() {
        lines.push(text("report.no_differences"));
    }
    for tr in &report.tables {
        lines.push(message(
            "report.table_line",
            &[
                ("table", &tr.table_name),
                ("inserts", &tr.inserts.to_string()),
                ("updates", &tr.updates.to_string()),
                ("deletes", &tr.deletes.to_string()),
            ],
        ));
    }
    if let Some(ref path) = report.master_script {
        lines.push(message("report.master_script", &[("path", path)]));
    }
    if !report.warnings.is_empty() {
        lines.push(text("report.warnings_header"));
        for w in &report.warnings {
            lines.push(message("report.warning_line", &[("message", w)]));
        }
    }
    lines
}

/// Print the diff report to stderr.
fn print_report(report: &diff_data::DiffReport) {
    for line in report_lines(report) {
        eprintln!("{}", line);
    }
}

/// Core async logic, separated from main for clean error handling.
async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(ref locale) = cli.locale {
        diff_data::i18n::set_lang(locale);
    }

    let reference_pool = diff_data::create_pool(&cli.reference_url)
        .await
        .map_err(|e| message("error.reference_db", &[("err", &e.to_string())]))?;
    let target_pool = diff_data::create_pool(&cli.target_url)
        .await
        .map_err(|e| message("error.target_db", &[("err", &e.to_string())]))?;

    let tables = parse_table_pairs(&cli.table);
    let alternate_keys = parse_alternate_keys(&cli.alternate_key)?;

    let include_identity_columns = if cli.include_identity {
        Some(true)
    } else if cli.exclude_identity {
        Some(false)
    } else {
        None
    };

    let output_dir = cli.output_dir.clone().unwrap_or_else(|| {
        cli.output.parent().map(|p| p.to_path_buf()).unwrap_or_else(|| PathBuf::from("."))
    });

    let show_progress = !cli.no_progress;
    let on_progress: Option<Arc<dyn Fn(diff_data::config::ProgressEvent) + Send + Sync>> =
        if show_progress { Some(Arc::new(|event| print_progress(&event))) } else { None };

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
        exclude_tables: cli.exclude_table.into_iter().map(|t| t.to_lowercase()).collect::<HashSet<String>>(),
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
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();

    // the language before the command line, so that --help is in it
    detect_and_set_locale();
    let matches = apply_i18n().get_matches();
    let cli = Cli::from_arg_matches(&matches).expect("failed to parse CLI arguments");

    if let Err(e) = run(cli).await {
        // a diff_data error starts with its code
        eprintln!("{}", e);
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use diff_data::config::ProgressEvent;

    #[test]
    fn a_table_is_one_name_or_a_reference_and_a_target() {
        let pairs = parse_table_pairs(&["users".to_string(), "public.a:archive.b".to_string()]);
        assert_eq!((pairs[0].reference.as_str(), pairs[0].target.as_str()), ("users", "users"));
        assert_eq!((pairs[1].reference.as_str(), pairs[1].target.as_str()), ("public.a", "archive.b"));
    }

    #[test]
    fn an_alternate_key_is_a_table_and_its_columns() {
        let keys = parse_alternate_keys(&["users:email, tenant".to_string()]).unwrap();
        assert_eq!(keys["users"], vec!["email", "tenant"]);
        diff_data::i18n::set_lang("en");
        assert!(parse_alternate_keys(&["no-colon".to_string()]).unwrap_err().starts_with("DA-E020 alternate key no-colon"));
    }

    #[test]
    fn every_progress_event_prints() {
        let events = vec![
            ProgressEvent::TableStart { table: "t".into(), table_index: 0, total_tables: 2 },
            ProgressEvent::RowProcessed { table: "t".into(), row_number: 999 },
            ProgressEvent::RowProcessed { table: "t".into(), row_number: 2000 },
            ProgressEvent::TableDiffComplete { table: "t".into(), inserts: 1, updates: 2 },
            ProgressEvent::TableDeleteComplete { table: "t".into(), deletes: 3 },
            ProgressEvent::SortingDependencies,
            ProgressEvent::EstimationStart { total_tables: 1 },
            ProgressEvent::EstimationTableDone { table: "t".into(), estimated_seconds: 0.5, strategy: "IdChunkSmall".into() },
            ProgressEvent::EstimationComplete { total_estimated_seconds: 0.5 },
            ProgressEvent::ChunkProgress { table: "t".into(), chunk_index: 0, total_chunks: 3 },
        ];
        diff_data::i18n::set_lang("en");
        for e in &events {
            print_progress(e);
        }
        assert_eq!(progress_line(&events[0]).unwrap(), "[1/2] t");
        assert!(progress_line(&events[1]).is_none(), "between thousands");
        assert_eq!(progress_line(&events[2]).unwrap(), "t: 2000 rows");
        assert!(events.iter().filter_map(progress_line).all(|l| !l.contains("progress.")), "each has its text");
    }

    #[test]
    fn a_report_prints_its_tables_its_script_and_its_warnings() {
        diff_data::i18n::set_lang("en");
        print_report(&diff_data::DiffReport::default());
        assert_eq!(report_lines(&diff_data::DiffReport::default()), vec!["Data diff:", "  no differences"]);
        let report = diff_data::DiffReport {
            tables: vec![diff_data::TableDiffReport { table_name: "t".into(), inserts: 1, ..Default::default() }],
            warnings: vec!["careful".into()],
            master_script: Some("out.sql".into()),
        };
        print_report(&report);
        assert_eq!(report_lines(&report), vec![
            "Data diff:", "  t: 1 inserts, 0 updates, 0 deletes", "SQL: out.sql", "Warnings:", "  careful",
        ]);
    }

    #[test]
    fn the_command_line_parses_with_its_help_text() {
        detect_and_set_locale();
        diff_data::i18n::set_lang("en");
        let cmd = apply_i18n();
        let about = cmd.get_about().unwrap().to_string();
        assert!(about.contains("use sge") && !about.starts_with("cli."), "{}", about);
        assert!(cmd.get_arguments().all(|a| a.get_help().is_none_or(|h| !h.to_string().starts_with("cli."))), "every option has its help");
        let matches = cmd
            .try_get_matches_from([
                "data-diff", "-r", "host=a", "-t", "host=b", "-o", "out/master.sql",
                "--table", "users", "--table", "a:b", "--alternate-key", "users:email",
                "--include-identity", "--no-progress", "--chunk-size", "5",
            ])
            .unwrap();
        let cli = Cli::from_arg_matches(&matches).unwrap();
        assert_eq!(cli.table, vec!["users", "a:b"]);
        assert_eq!(cli.chunk_size, 5);
        assert!(cli.include_identity && cli.no_progress && !cli.exclude_identity);
        assert!(
            apply_i18n()
                .try_get_matches_from(["data-diff", "-r", "a", "-t", "b", "-o", "o", "--include-identity", "--exclude-identity"])
                .is_err()
        );
    }

    #[tokio::test]
    async fn a_bad_connection_string_is_refused_before_anything_runs() {
        let cli = Cli::parse_from(["data-diff", "-r", "host=a port=notaport", "-t", "host=b", "-o", "o.sql"]);
        assert!(run(cli).await.is_err());
        let cli = Cli::parse_from(["data-diff", "-r", "host=a", "-t", "port=notaport", "-o", "o.sql", "--locale", "en"]);
        assert!(run(cli).await.is_err());
        let cli = Cli::parse_from(["data-diff", "-r", "host=a", "-t", "host=b", "-o", "o.sql", "--alternate-key", "bad"]);
        assert!(run(cli).await.is_err());
    }
}

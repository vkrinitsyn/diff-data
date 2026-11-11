# data-diff

[![crates.io](https://img.shields.io/crates/v/diff_data.svg)](https://crates.io/crates/diff_data)
[![audit](https://github.com/vkrinitsyn/diff-data/actions/workflows/audit.yml/badge.svg)](https://github.com/vkrinitsyn/diff-data/actions/workflows/audit.yml)
[![trivy](https://github.com/vkrinitsyn/diff-data/actions/workflows/trivy.yml/badge.svg)](https://github.com/vkrinitsyn/diff-data/actions/workflows/trivy.yml)
[![coverage](https://raw.githubusercontent.com/vkrinitsyn/diff-data/badges/coverage.svg?sanitize=true)](https://github.com/vkrinitsyn/diff-data/actions/workflows/coverage.yml)
[![license](https://img.shields.io/github/license/vkrinitsyn/diff-data.svg)](LICENSE)

PostgreSQL data diff: the SQL (INSERT, UPDATE, DELETE) that makes a target database's rows a reference's,
as a library any project calls (sge, ytctl) and as a command line, `diff_data`.

**The data only.** Schemas are compared by sge (Schema Guard Enterprise, `sge::dbm_compare`): when a
comprehensive diff is required - schema and data - use sge, for the schema, with diff_data for the data.
diff_data checks only what a data diff needs of a schema: that the target has each reference column, and
which key a row is matched by.

## As a library

```toml
diff_data = { version = "0.4", default-features = false }   # without the command line (clap)
```

`diff_data::run_diff(DiffConfig)` runs the diff; `DataDiff` its parts: the tables (the configured pairs, else
every table of `reference_schema`), each table's chunk strategy and the estimated time, the per-table diff,
and the output - a file per table and kind with a master script, one file (`single_file`), or a stream
(`output_stream`). With `check_dependencies` (the default) the tables go parents first by their foreign keys,
and every DELETE comes after every INSERT and UPDATE, children first: the SQL applies as written.

## Messages and error codes

Every message is in English, Russian and - not reviewed - French, German, Simplified Chinese and Japanese:
`locales/<lang>.yml`, one file per language. The language is `diff_data::i18n::set_lang`'s, else `LANG`'s
(the command line: `--locale`, else `DIFF_DATA_LOCALE`). An error starts with its code, `DA-Ennn`, the same
in every language and described at [docs.dbinvent.com](https://docs.dbinvent.com) (`DD-` is diff-doc's,
`SGE-` sge's). A new error takes the next code; a code is never reused.

## Identify chunk strategy:

1. figure out a timestamp column name: update* if not exists then create* timestamp,
   -or use a DiffConfig defined column name for common a timestamp column name.

2. table size:
    - if more than 1M row, than if no timestamp column and table not in exclude list, than suggest to:
        - define a timestamp column name (see above)
        - or add table to exclude list
    - if more than 1M row and timestamp column defined, than:
        - use md5 hash of chunk of tables split by DiffConfig defined max % chunk size (default 10%) to identify chunk of data,
        - found as max(timestamp column) - min (timstamp column) in days / 10, than use this chunk size to split table into chunks, and compare each chunk separately.
          sample SQL for timestamp chunks: SELECT md5(t.*::text) FROM <table_name> where timestamp > $1 and timestamp <= $2;
    - if less than 1M row, than compare table as split by id chunks
      sample SQL for id chunks: SELECT md5(t.*::text) FROM <table_name> where id in ($1) ;
    - if less than 100K row, than compare table as split by id chunks
      sample SQL for id: SELECT md5(t.*::text) FROM <table_name> where id in ($1) ;

3: ETC(completion) estimation:
- check each tables total size, define timestamp column, try to md5 a 1/10 of chunk but more than 100K rows, and estimate time for each chunk, than sum up for total ETC estimation.
- if takes more than 3 hour, than exist with a message of ETC and request a special flag for force a task run.
- progress flags is on by default, but if --no-progress flag is set, than no progress output.

## Split the job into multiple tasks:
 - Also there is a feature for checksum calculation only and a feature for generating sql based on provided calculated md5 from previous step.
 - if specific flag set then md5 checksum of calculated chunks with a strategies will send into another output format ot use to compare one db with another. 
 - This allow to do md5 calculation for two db separately in background, then compare result and proceed with generating sql content. 

## Compare across a cluster (`cluster`)

The rest of this crate compares two databases, a reference and a target. `diff_data::cluster` compares
one table across N copies - the nodes of a cluster - as YT Cluster does it (2026-11-11):

- `cluster::recon` - YT Cluster's background reconciliation (ytserv), the reference side: a row's id (its
  key columns as text joined by `|`), the value a row is compared by (`to_jsonb(t.*)` less the ignored
  columns), a row's 32-bit checksum, frames of rows folded as `(count, xor32, sum64)` over an integer key,
  and the vote on a row's copies - unanimous, a strict majority, or on a table its writer commits alone
  the copy of the node that wrote the row last; else the row is blocked.
- `cluster::diff` - a node made the same as a primary (YT Cluster's ytctl), on recon's id, value and fold:
  phase 1 folds the whole table on every node; phase 2 keys each node's rows, plans upserts of the
  primary's rows that differ or are missing and deletes of the node's rows the primary lacks, and writes
  one SQL file per node. A table over 1M rows is folded by key range, a differing range halved until it
  is small, a one-key range paged by its second key column. Two rows of one id refuse the table.
- `cluster::placement` - which nodes of a YT Cluster hold a table (its `yt_info('P')` row), and why a
  table cannot be repaired from one primary (sharded, held by zone, another database).

The SQL builders and the vote are pure; the reads take a `tokio_postgres::Client`.

## Tests

`cargo test` runs the unit tests alone. The live tests run against a PostgreSQL of your own, as a superuser
(a "large" table is one whose `pg_class.reltuples` a test sets past 1M rows):
`DIFF_DATA_PG="host=127.0.0.1 port=5432 user=postgres" cargo test`. Without `DIFF_DATA_PG` they do nothing.
Each creates and drops its own databases, all named `dd_*`:

- `tests/diff_live.rs` - the reference/target diff: `DataDiff` in file, stream, single-file and checksum modes,
  a whole schema by its foreign keys, every column type round-tripped, and its parts;
- `tests/cluster_live.rs` - the cluster compare (`dd_cluster_primary`, `dd_cluster_node`, `dd_edges_*`);
- `tests/cli.rs` - the `diff_data` binary, run as a user runs it.

Line coverage: `DIFF_DATA_PG=... cargo llvm-cov`; CI fails under 90%.

# Contributing to data-diff (diff_data)

Thank you for helping. diff_data is MIT licensed; a contribution is accepted under the
same license (see [LICENSE](LICENSE)).

## Issues

Open an issue on GitHub for a bug or a feature request. For a bug, give the diff_data
version (Cargo.toml's, or the commit you built), the PostgreSQL versions of the reference
and the target, the command you ran and its output, and the smallest tables (their DDL
and a few rows) that show it.

A security problem is not reported in a public issue: see [SECURITY.md](SECURITY.md).

## Pull requests

1. Fork the repository and branch from `main`.
2. Keep a change to one subject; describe what it changes and why.
3. Build and test it (below); add tests for what you change.
4. Messages a user reads go through rust-i18n (`t!("...")` in `src/main.rs`, the locale
   files in `locales/`): add a new key to every one of them.
5. Open the pull request against `main`.

## Build and test

```
cargo build --release      # target/release/diff_data
cargo test                 # the unit tests; the live test does nothing without DIFF_DATA_PG
```

The live test (`tests/cluster_live.rs`) runs against a PostgreSQL of your own, given by
`DIFF_DATA_PG`; it creates and drops the databases `dd_cluster_primary` and
`dd_cluster_node`:

```
DIFF_DATA_PG="host=127.0.0.1 port=5432 user=postgres" cargo test --test cluster_live
```

CI (`.github/workflows/coverage.yml`) runs it on a throwaway PostgreSQL.

Before a pull request, run the checks as well:

```
cargo audit      # known vulnerabilities in the pinned dependencies
cargo geiger     # unsafe code in diff_data and its dependencies
```

## Code of conduct

Taking part in this project means following its
[Code of Conduct](CODE_OF_CONDUCT.md).

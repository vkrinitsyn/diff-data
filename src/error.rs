use std::{fmt, io};

/// What stops a diff. Its message (Display) starts with its code, `DA-Ennn`, and is in the language of
/// [crate::i18n::lang]; `Config` and `Checksum` carry theirs, made where they arose.
#[derive(Debug)]
pub enum DataDiffError {
    Postgres(tokio_postgres::Error),
    Io(io::Error),
    Pool(bb8::RunError<tokio_postgres::Error>),
    NoPrimaryKey(String),
    TableNotFound(String),
    ColumnMismatch {
        ref_table: String,
        target_table: String,
    },
    Cancelled,
    /// a configuration the diff cannot run with, or a table it cannot diff as configured: its message
    Config(String),
    /// a checksum file that cannot be read: its message
    Checksum(String),
    Yaml(serde_yaml::Error),
}

impl fmt::Display for DataDiffError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let m = match self {
            DataDiffError::Postgres(e) => tr!("error.postgres", err = crate::cluster::diff::pg_text(e)),
            DataDiffError::Io(e) => tr!("error.io", err = e),
            DataDiffError::Pool(e) => tr!("error.pool", err = e),
            DataDiffError::NoPrimaryKey(t) => tr!("error.no_primary_key", table = t),
            DataDiffError::TableNotFound(t) => tr!("error.table_not_found", table = t),
            DataDiffError::ColumnMismatch { ref_table, target_table } => {
                tr!("error.column_mismatch", reference = ref_table, target = target_table)
            }
            DataDiffError::Cancelled => tr!("error.cancelled"),
            DataDiffError::Config(m) | DataDiffError::Checksum(m) => m.clone(),
            DataDiffError::Yaml(e) => tr!("error.yaml", err = e),
        };
        f.write_str(&m)
    }
}

impl std::error::Error for DataDiffError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DataDiffError::Postgres(e) => Some(e),
            DataDiffError::Io(e) => Some(e),
            DataDiffError::Pool(e) => Some(e),
            DataDiffError::Yaml(e) => Some(e),
            _ => None,
        }
    }
}

impl From<tokio_postgres::Error> for DataDiffError {
    fn from(e: tokio_postgres::Error) -> Self {
        DataDiffError::Postgres(e)
    }
}

impl From<io::Error> for DataDiffError {
    fn from(e: io::Error) -> Self {
        DataDiffError::Io(e)
    }
}

impl From<bb8::RunError<tokio_postgres::Error>> for DataDiffError {
    fn from(e: bb8::RunError<tokio_postgres::Error>) -> Self {
        DataDiffError::Pool(e)
    }
}

impl From<serde_yaml::Error> for DataDiffError {
    fn from(e: serde_yaml::Error) -> Self {
        DataDiffError::Yaml(e)
    }
}

pub type Result<T> = std::result::Result<T, DataDiffError>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn each_error_says_what_went_wrong_after_its_code() {
        crate::i18n::set_lang("en");
        assert_eq!(DataDiffError::NoPrimaryKey("t".into()).to_string(), "DA-E004 table t has no primary key");
        assert_eq!(DataDiffError::TableNotFound("t".into()).to_string(), "DA-E005 table not found: t");
        assert_eq!(
            DataDiffError::ColumnMismatch { ref_table: "a".into(), target_table: "b".into() }.to_string(),
            "DA-E006 the target table b lacks columns of the reference table a"
        );
        assert_eq!(DataDiffError::Cancelled.to_string(), "DA-E007 the diff was cancelled");
        assert_eq!(DataDiffError::Config("DA-E011 c".into()).to_string(), "DA-E011 c");
        assert_eq!(DataDiffError::Checksum("DA-E017 k".into()).to_string(), "DA-E017 k");
        let io: DataDiffError = io::Error::new(io::ErrorKind::NotFound, "gone").into();
        assert_eq!(io.to_string(), "DA-E002 I/O error: gone");
        assert!(io.source().is_some());
        let yaml: DataDiffError = serde_yaml::from_str::<Vec<u8>>("{").unwrap_err().into();
        assert!(yaml.to_string().starts_with("DA-E008 ") && yaml.source().is_some());
        let pool: DataDiffError = bb8::RunError::<tokio_postgres::Error>::TimedOut.into();
        assert!(pool.to_string().starts_with("DA-E003 ") && pool.source().is_some());
        assert!(DataDiffError::Cancelled.source().is_none());
    }
}

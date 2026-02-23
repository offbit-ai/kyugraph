use thiserror::Error;

/// Top-level error type for the entire KyuGraph engine.
/// Each variant corresponds to a distinct subsystem, matching
/// the Kuzu/RyuGraph exception hierarchy.
#[derive(Error, Debug)]
pub enum KyuError {
    #[error("parse error: {0}")]
    Parser(String),

    #[error("binder error: {0}")]
    Binder(String),

    #[error("catalog error: {0}")]
    Catalog(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("transaction error: {0}")]
    Transaction(String),

    #[error("runtime error: {0}")]
    Runtime(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("io error: {source}")]
    Io {
        #[from]
        source: std::io::Error,
    },

    #[error("overflow error: {0}")]
    Overflow(String),

    #[error("copy error: {0}")]
    Copy(String),

    #[error("delta error: {0}")]
    Delta(String),

    #[error("extension error: {0}")]
    Extension(String),

    #[error("not implemented: {0}")]
    NotImplemented(String),

    #[error("interrupted")]
    Interrupted,
}

pub type KyuResult<T> = Result<T, KyuError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let kyu_err: KyuError = io_err.into();
        assert!(matches!(kyu_err, KyuError::Io { .. }));
        assert!(kyu_err.to_string().contains("file not found"));
    }

    #[test]
    fn display_formatting() {
        let err = KyuError::Parser("unexpected token".to_string());
        assert_eq!(err.to_string(), "parse error: unexpected token");

        let err = KyuError::Interrupted;
        assert_eq!(err.to_string(), "interrupted");
    }

    #[test]
    fn result_alias_works() {
        fn returns_ok() -> KyuResult<i32> {
            Ok(42)
        }
        fn returns_err() -> KyuResult<i32> {
            Err(KyuError::Internal("oops".into()))
        }
        assert_eq!(returns_ok().unwrap(), 42);
        assert!(returns_err().is_err());
    }
}

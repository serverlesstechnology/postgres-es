use std::fmt::{Debug, Display, Formatter};

use cqrs_es::persist::PersistenceError;
use cqrs_es::AggregateError;
use sqlx::Error;

#[derive(Debug, PartialEq)]
pub enum PostgresAggregateError {
    OptimisticLockError,
    UnknownError(String),
}

impl Display for PostgresAggregateError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PostgresAggregateError::OptimisticLockError => write!(f, "optimistic lock error"),
            PostgresAggregateError::UnknownError(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for PostgresAggregateError {}

impl From<sqlx::Error> for PostgresAggregateError {
    fn from(err: sqlx::Error) -> Self {
        // TODO: improve error handling
        match &err {
            Error::Configuration(_) => {}
            Error::Database(database_error) => {
                if let Some(code) = database_error.code() {
                    if code.as_ref() == "23505" {
                        return PostgresAggregateError::OptimisticLockError;
                    }
                }
            }
            Error::Io(_) => {}
            Error::Tls(_) => {}
            Error::Protocol(_) => {}
            Error::RowNotFound => {}
            Error::TypeNotFound { .. } => {}
            Error::ColumnIndexOutOfBounds { .. } => {}
            Error::ColumnNotFound(_) => {}
            Error::ColumnDecode { .. } => {}
            Error::Decode(_) => {}
            Error::PoolTimedOut => {}
            Error::PoolClosed => {}
            Error::WorkerCrashed => {}
            Error::Migrate(_) => {}
            _ => {}
        };
        PostgresAggregateError::UnknownError(format!("{:?}", err))
    }
}

impl From<PostgresAggregateError> for AggregateError {
    fn from(err: PostgresAggregateError) -> Self {
        match err {
            PostgresAggregateError::OptimisticLockError => {
                AggregateError::TechnicalError(err.to_string())
            }
            PostgresAggregateError::UnknownError(msg) => AggregateError::TechnicalError(msg),
        }
    }
}

impl From<serde_json::Error> for PostgresAggregateError {
    fn from(err: serde_json::Error) -> Self {
        PostgresAggregateError::UnknownError(err.to_string())
    }
}

impl From<PostgresAggregateError> for PersistenceError {
    fn from(err: PostgresAggregateError) -> Self {
        match err {
            PostgresAggregateError::OptimisticLockError => PersistenceError::OptimisticLockError,
            PostgresAggregateError::UnknownError(msg) => PersistenceError::UnknownError(msg),
        }
    }
}

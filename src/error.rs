use std::fmt::{Debug, Display, Formatter};

use cqrs_es::AggregateError;
use sqlx::Error;

#[derive(Debug, PartialEq)]
pub struct PostgresAggregateError(String);

impl Display for PostgresAggregateError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
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
                        return PostgresAggregateError("optimistic lock error".to_string());
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
        PostgresAggregateError(format!("{:?}", err))
    }
}

impl From<PostgresAggregateError> for AggregateError {
    fn from(err: PostgresAggregateError) -> Self {
        AggregateError::TechnicalError(err.0)
    }
}

impl From<serde_json::Error> for PostgresAggregateError {
    fn from(err: serde_json::Error) -> Self {
        PostgresAggregateError(err.to_string())
    }
}

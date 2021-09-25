use cqrs_es::{Aggregate, CqrsFramework, Query};

use crate::snapshot_store::PostgresSnapshotStore;
use crate::{PostgresCqrs, PostgresSnapshotCqrs, PostgresStore};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};

/// A convenience building a simple connection pool for PostgresDb.
pub async fn default_postgress_pool(connection_string: &str) -> Pool<Postgres> {
    PgPoolOptions::new()
        .max_connections(10)
        .connect(connection_string)
        .await
        .expect("unable to connect to database")
}

/// A convenience function for creating a CqrsFramework from a database connection pool
/// and queries.
pub fn postgres_cqrs<A>(
    pool: Pool<Postgres>,
    query_processor: Vec<Box<dyn Query<A>>>,
) -> PostgresCqrs<A>
where
    A: Aggregate,
{
    let store = PostgresStore::new(pool);
    CqrsFramework::new(store, query_processor)
}

/// A convenience function for creating a CqrsFramework using a snapshot store.
pub fn postgres_snapshot_cqrs<A>(
    pool: Pool<Postgres>,
    query_processor: Vec<Box<dyn Query<A>>>,
) -> PostgresSnapshotCqrs<A>
where
    A: Aggregate,
{
    let store = PostgresSnapshotStore::new(pool);
    CqrsFramework::new(store, query_processor)
}

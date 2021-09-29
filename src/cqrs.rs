use cqrs_es::persist::{PersistedEventStore, PersistedSnapshotStore};
use cqrs_es::{Aggregate, CqrsFramework, Query};
use std::sync::Arc;

use crate::event_repository::PostgresEventRepository;
use crate::snapshot_repository::PostgresSnapshotRepository;
use crate::{PostgresCqrs, PostgresSnapshotCqrs};
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
    query_processor: Vec<Arc<dyn Query<A>>>,
) -> PostgresCqrs<A>
where
    A: Aggregate,
{
    let repo = PostgresEventRepository::new(pool);
    let store = PersistedEventStore::new(repo);
    CqrsFramework::new(store, query_processor)
}

/// A convenience function for creating a CqrsFramework using a snapshot store.
pub fn postgres_snapshot_cqrs<A>(
    pool: Pool<Postgres>,
    query_processor: Vec<Arc<dyn Query<A>>>,
) -> PostgresSnapshotCqrs<A>
where
    A: Aggregate,
{
    let repo = PostgresEventRepository::new(pool.clone());
    let snapshot_repo = PostgresSnapshotRepository::new(pool);
    let store = PersistedSnapshotStore::new(snapshot_repo, repo);
    CqrsFramework::new(store, query_processor)
}

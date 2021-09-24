use cqrs_es::{Aggregate, CqrsFramework, QueryProcessor};

use crate::snapshot_store::{PostgresSnapshotStore, PostgresSnapshotStoreAggregateContext};
use crate::{PostgresStore, PostgresStoreAggregateContext};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};

/// A convenience type for creating a CqrsFramework backed by PostgresStore and using a simple
/// metadata supplier with time of commit.
pub type PostgresCqrs<A> = CqrsFramework<A, PostgresStore<A>, PostgresStoreAggregateContext<A>>;

/// A convenience type for creating a CqrsFramework backed by PostgresSnapshotStore and using a
/// simple metadata supplier with time of commit.
pub type PostgresSnapshotCqrs<A> =
    CqrsFramework<A, PostgresSnapshotStore<A>, PostgresSnapshotStoreAggregateContext<A>>;

/// A convenience building a connection pool for PostgresDb.
pub async fn default_postgress_pool(connection_string: &str) -> Pool<Postgres> {
    PgPoolOptions::new()
        .max_connections(10)
        .connect(connection_string)
        .await
        .expect("unable to connect to database")
}

/// A convenience function for creating a CqrsFramework
pub fn postgres_cqrs<A>(
    pool: Pool<Postgres>,
    query_processor: Vec<Box<dyn QueryProcessor<A>>>,
) -> PostgresCqrs<A>
where
    A: Aggregate,
{
    let store = PostgresStore::new(pool);
    CqrsFramework::new(store, query_processor)
}

/// A convenience function for creating a CqrsFramework using a snapshot store
pub fn postgres_snapshot_cqrs<A>(
    pool: Pool<Postgres>,
    query_processor: Vec<Box<dyn QueryProcessor<A>>>,
) -> PostgresSnapshotCqrs<A>
where
    A: Aggregate,
{
    let store = PostgresSnapshotStore::new(pool);
    CqrsFramework::new(store, query_processor)
}

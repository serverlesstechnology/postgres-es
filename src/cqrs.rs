use cqrs_es::persist::{PersistedEventStore, SourceOfTruth};
use cqrs_es::{Aggregate, CqrsFramework, Query};
use std::sync::Arc;

use crate::{PostgresCqrs, PostgresEventRepository};
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
    snapshot_size: usize,
) -> PostgresCqrs<A>
where
    A: Aggregate,
{
    let repo = PostgresEventRepository::new(pool);
    let store =
        PersistedEventStore::new(repo).with_storage_method(SourceOfTruth::Snapshot(snapshot_size));
    CqrsFramework::new(store, query_processor)
}

/// A convenience function for creating a CqrsFramework using an aggregate store.
pub fn postgres_aggregate_cqrs<A>(
    pool: Pool<Postgres>,
    query_processor: Vec<Arc<dyn Query<A>>>,
) -> PostgresCqrs<A>
where
    A: Aggregate,
{
    let repo = PostgresEventRepository::new(pool);
    let store = PersistedEventStore::new(repo).with_storage_method(SourceOfTruth::AggregateStore);
    CqrsFramework::new(store, query_processor)
}

#[cfg(test)]
mod test {
    use crate::testing::tests::{
        TestAggregate, TestQueryRepository, TestView, TEST_CONNECTION_STRING,
    };
    use crate::{default_postgress_pool, postgres_cqrs, PostgresViewRepository};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_valid_cqrs_framework() {
        let pool = default_postgress_pool(TEST_CONNECTION_STRING).await;
        let repo =
            PostgresViewRepository::<TestView, TestAggregate>::new("test_query", pool.clone());
        let query = TestQueryRepository::new(repo);
        let _ps = postgres_cqrs(pool, vec![Arc::new(query)]);
    }
}

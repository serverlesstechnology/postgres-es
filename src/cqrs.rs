use cqrs_es::{Aggregate, CqrsFramework, DomainEvent, QueryProcessor};
use postgres::Connection;

use crate::{PostgresStore, PostgresStoreAggregateContext};
use crate::aggregate_store::{PostgresSnapshotStore, PostgresSnapshotStoreAggregateContext};

/// A convenience type for creating a CqrsFramework backed by PostgresStore and using a simple
/// metadata supplier with time of commit.
pub type PostgresCqrs<A, E> = CqrsFramework<A, E, PostgresStore<A, E>, PostgresStoreAggregateContext<A>>;

/// A convenience type for creating a CqrsFramework backed by PostgresSnapshotStore and using a
/// simple metadata supplier with time of commit.
pub type PostgresSnapshotCqrs<A, E> = CqrsFramework<A, E, PostgresSnapshotStore<A, E>, PostgresSnapshotStoreAggregateContext<A>>;

/// A convenience function for creating a CqrsFramework
pub fn postgres_cqrs<A, E>(conn: Connection, query_processor: Vec<Box<dyn QueryProcessor<A, E>>>) -> PostgresCqrs<A, E>
    where A: Aggregate,
          E: DomainEvent<A>
{
    let store = PostgresStore::new(conn);
    CqrsFramework::new(store, query_processor)
}

/// A convenience function for creating a CqrsFramework using a snapshot store
pub fn postgres_snapshot_cqrs<A, E>(conn: Connection, query_processor: Vec<Box<dyn QueryProcessor<A, E>>>) -> PostgresSnapshotCqrs<A, E>
    where A: Aggregate,
          E: DomainEvent<A>
{
    let store = PostgresSnapshotStore::new(conn);
    CqrsFramework::new(store, query_processor)
}


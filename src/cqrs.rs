use cqrs_es::{Aggregate, CqrsFramework, QueryProcessor};
use postgres::Connection;

use crate::{PostgresStore, PostgresStoreAggregateContext};
use crate::aggregate_store::{PostgresSnapshotStore, PostgresSnapshotStoreAggregateContext};

/// A convenience type for creating a CqrsFramework backed by PostgresStore and using a simple
/// metadata supplier with time of commit.
pub type PostgresCqrs<A> = CqrsFramework<A, PostgresStore<A>, PostgresStoreAggregateContext<A>>;

/// A convenience type for creating a CqrsFramework backed by PostgresSnapshotStore and using a
/// simple metadata supplier with time of commit.
pub type PostgresSnapshotCqrs<A> = CqrsFramework<A, PostgresSnapshotStore<A>, PostgresSnapshotStoreAggregateContext<A>>;

/// A convenience function for creating a CqrsFramework
pub fn postgres_cqrs<A>(conn: Connection, query_processor: Vec<Box<dyn QueryProcessor<A>>>) -> PostgresCqrs<A>
    where A: Aggregate,
{
    let store = PostgresStore::new(conn);
    CqrsFramework::new(store, query_processor)
}

/// A convenience function for creating a CqrsFramework using a snapshot store
pub fn postgres_snapshot_cqrs<A>(conn: Connection, query_processor: Vec<Box<dyn QueryProcessor<A>>>) -> PostgresSnapshotCqrs<A>
    where A: Aggregate
{
    let store = PostgresSnapshotStore::new(conn);
    CqrsFramework::new(store, query_processor)
}


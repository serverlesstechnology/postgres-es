use cqrs_es::{Aggregate, CqrsFramework, QueryProcessor};

use crate::snapshot_store::{PostgresSnapshotStore, PostgresSnapshotStoreAggregateContext};
use crate::{PostgresStore, PostgresStoreAggregateContext, EventRepository, SnapshotRepository};

/// A convenience type for creating a CqrsFramework backed by PostgresStore and using a simple
/// metadata supplier with time of commit.
pub type PostgresCqrs<A> = CqrsFramework<A, PostgresStore<A>, PostgresStoreAggregateContext<A>>;

/// A convenience type for creating a CqrsFramework backed by PostgresSnapshotStore and using a
/// simple metadata supplier with time of commit.
pub type PostgresSnapshotCqrs<A> =
    CqrsFramework<A, PostgresSnapshotStore<A>, PostgresSnapshotStoreAggregateContext<A>>;

/// A convenience function for creating a CqrsFramework
pub fn postgres_cqrs<A>(
    repo: EventRepository<A>,
    query_processor: Vec<Box<dyn QueryProcessor<A>>>,
) -> PostgresCqrs<A>
where
    A: Aggregate,
{
    let store = PostgresStore::new(repo);
    CqrsFramework::new(store, query_processor)
}

/// A convenience function for creating a CqrsFramework using a snapshot store
pub fn postgres_snapshot_cqrs<A>(
    repo: SnapshotRepository<A>,
    event_repo: EventRepository<A>,
    query_processor: Vec<Box<dyn QueryProcessor<A>>>,
) -> PostgresSnapshotCqrs<A>
where
    A: Aggregate,
{
    let store = PostgresSnapshotStore::new(repo, event_repo);
    CqrsFramework::new(store, query_processor)
}

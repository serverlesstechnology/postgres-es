use crate::event_repository::PostgresEventRepository;
use crate::snapshot_repository::PostgresSnapshotRepository;
use cqrs_es::persist::{PersistedEventStore, PersistedSnapshotStore};
use cqrs_es::CqrsFramework;

/// A convenience type for a CqrsFramework backed by
/// [PostgresStore](struct.PostgresStore.html).
pub type PostgresCqrs<A> = CqrsFramework<A, PersistedEventStore<PostgresEventRepository<A>, A>>;

/// A convenience type for a CqrsFramework backed by
/// [PostgresSnapshotStore](struct.PostgresSnapshotStore.html).
pub type PostgresSnapshotCqrs<A> = CqrsFramework<
    A,
    PersistedSnapshotStore<PostgresEventRepository<A>, PostgresSnapshotRepository<A>, A>,
>;

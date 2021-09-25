use crate::{PostgresSnapshotStore, PostgresStore};
use cqrs_es::CqrsFramework;

/// A convenience type for a CqrsFramework backed by
/// [PostgresStore](struct.PostgresStore.html).
pub type PostgresCqrs<A> = CqrsFramework<A, PostgresStore<A>>;

/// A convenience type for a CqrsFramework backed by
/// [PostgresSnapshotStore](struct.PostgresSnapshotStore.html).
pub type PostgresSnapshotCqrs<A> = CqrsFramework<A, PostgresSnapshotStore<A>>;

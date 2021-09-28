use std::collections::HashMap;
use std::marker::PhantomData;

use async_trait::async_trait;
use cqrs_es::{Aggregate, AggregateContext, AggregateError, EventEnvelope, EventStore};
use sqlx::{Pool, Postgres};

use crate::event_repository::PostgresEventRepository;
use crate::snapshot_repository::PostgresSnapshotRepository;

/// Storage engine using a Postgres database backing.
/// This is an snapshot-sourced `EventStore`, meaning it uses the serialized aggregate as the
/// primary source of truth for the state of the aggregate.
///
/// The individual events are also persisted but are used only for updating queries.
///
/// For a event-sourced `EventStore` see [`PostgresStore`](struct.PostgresStore.html).
///
pub struct PostgresSnapshotStore<A: Aggregate> {
    repo: PostgresSnapshotRepository<A>,
    // TODO: combine event retrieval and remove EventRepository here
    event_repo: PostgresEventRepository<A>,
    _phantom: PhantomData<A>,
}

impl<A: Aggregate> PostgresSnapshotStore<A> {
    /// Creates a new `PostgresSnapshotStore` from the provided database connection pool,
    /// an `EventStore` used for configuring a new cqrs framework.
    ///
    /// ```ignore
    /// # use postgres_es::PostgresSnapshotStore;
    /// # use cqrs_es::CqrsFramework;
    /// let store = PostgresSnapshotStore::<MyAggregate>::new(pool);
    /// let cqrs = CqrsFramework::new(store, vec![]);
    /// ```
    pub fn new(pool: Pool<Postgres>) -> Self {
        let repo = PostgresSnapshotRepository::new(pool.clone());
        let event_repo = PostgresEventRepository::new(pool);
        PostgresSnapshotStore {
            repo,
            event_repo,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<A: Aggregate> EventStore<A> for PostgresSnapshotStore<A> {
    type AC = PostgresSnapshotStoreAggregateContext<A>;

    async fn load(&self, aggregate_id: &str) -> Vec<EventEnvelope<A>> {
        // TODO: combine with event store
        match self.event_repo.get_events(aggregate_id).await {
            Ok(val) => val,
            Err(_err) => {
                // TODO: improved error handling
                Default::default()
            }
        }
    }
    async fn load_aggregate(&self, aggregate_id: &str) -> PostgresSnapshotStoreAggregateContext<A> {
        match self.repo.get_snapshot(aggregate_id).await {
            Ok(snapshot) => match snapshot {
                Some(snapshot) => {
                    let _tmp = serde_json::to_string(&snapshot.aggregate).unwrap();
                    snapshot
                }
                None => PostgresSnapshotStoreAggregateContext {
                    aggregate_id: aggregate_id.to_string(),
                    aggregate: Default::default(),
                    current_sequence: 0,
                    current_snapshot: 0,
                },
            },
            Err(e) => {
                panic!("{}", e);
            }
        }
    }

    async fn commit(
        &self,
        events: Vec<A::Event>,
        mut context: PostgresSnapshotStoreAggregateContext<A>,
        metadata: HashMap<String, String>,
    ) -> Result<Vec<EventEnvelope<A>>, AggregateError> {
        for event in events.clone() {
            context.aggregate.apply(event);
        }
        let aggregate_id = context.aggregate_id.clone();
        let wrapped_events =
            self.wrap_events(&aggregate_id, context.current_sequence, events, metadata);

        if context.current_sequence == 0 {
            self.repo
                .insert(context.aggregate, aggregate_id, 1, &wrapped_events)
                .await?;
        } else {
            self.repo
                .update(
                    context.aggregate,
                    aggregate_id,
                    context.current_snapshot + 1,
                    &wrapped_events,
                )
                .await?;
        }

        Ok(wrapped_events)
    }
}

/// Holds context for the snapshot-sourced implementation PostgresSnapshotStore.
/// This is only used internally within the `EventStore`.
#[derive(Debug, PartialEq)]
pub struct PostgresSnapshotStoreAggregateContext<A>
where
    A: Aggregate,
{
    pub(crate) aggregate_id: String,
    pub(crate) aggregate: A,
    pub(crate) current_sequence: usize,
    pub(crate) current_snapshot: usize,
}

impl<A> AggregateContext<A> for PostgresSnapshotStoreAggregateContext<A>
where
    A: Aggregate,
{
    fn aggregate(&self) -> &A {
        &self.aggregate
    }
}

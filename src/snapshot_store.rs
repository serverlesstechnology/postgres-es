use std::collections::HashMap;
use std::marker::PhantomData;

use async_trait::async_trait;
use cqrs_es::{Aggregate, AggregateContext, AggregateError, EventEnvelope, EventStore};

use crate::event_repository::EventRepository;
use crate::snapshot_repository::SnapshotRepository;
use sqlx::{Pool, Postgres};

/// Storage engine using an Postgres backing and relying on a serialization of the aggregate rather
/// than individual events. This is similar to the "snapshot strategy" seen in many CQRS
/// frameworks.
pub struct PostgresSnapshotStore<A: Aggregate> {
    repo: SnapshotRepository<A>,
    // TODO: combine event retrieval and remove EventRepository here
    event_repo: EventRepository<A>,
    _phantom: PhantomData<A>,
}

impl<A: Aggregate> PostgresSnapshotStore<A> {
    /// Creates a new `PostgresSnapshotStore` from the provided database connection.
    pub fn new(pool: Pool<Postgres>) -> Self {
        let repo = SnapshotRepository::new(pool.clone());
        let event_repo = EventRepository::new(pool);
        PostgresSnapshotStore {
            repo,
            event_repo,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<A: Aggregate> EventStore<A, PostgresSnapshotStoreAggregateContext<A>>
    for PostgresSnapshotStore<A>
{
    async fn load(&self, aggregate_id: &str) -> Vec<EventEnvelope<A>> {
        // TODO: combine with store
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

/// Holds context for a pure event store implementation for MemStore
#[derive(Debug, PartialEq)]
pub struct PostgresSnapshotStoreAggregateContext<A>
where
    A: Aggregate,
{
    /// The aggregate ID of the aggregate instance that has been loaded.
    pub aggregate_id: String,
    /// The current state of the aggregate instance.
    pub(crate) aggregate: A,
    /// The last committed event sequence number for this aggregate instance.
    pub current_sequence: usize,
    /// The last committed snapshot version for this aggregate instance.
    pub current_snapshot: usize,
}

impl<A> AggregateContext<A> for PostgresSnapshotStoreAggregateContext<A>
where
    A: Aggregate,
{
    fn aggregate(&self) -> &A {
        &self.aggregate
    }
}

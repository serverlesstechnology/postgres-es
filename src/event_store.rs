use async_trait::async_trait;
use std::collections::HashMap;
use std::marker::PhantomData;

use crate::event_repository::EventRepository;
use cqrs_es::{Aggregate, AggregateContext, AggregateError, EventEnvelope, EventStore};
use sqlx::{Pool, Postgres};

/// Storage engine using an Postgres backing. This is the only persistent store currently
/// provided.
pub struct PostgresStore<A: Aggregate + Send + Sync> {
    repo: EventRepository<A>,
    _phantom: PhantomData<A>,
}

impl<A: Aggregate> PostgresStore<A> {
    /// Creates a new `PostgresStore` from the provided database connection,
    /// an `EventStore` used for configuring a new cqrs framework.
    ///
    /// This is an event sourced `EventStore`, meaning all previous events for the
    /// aggregate instance will be loaded before processing a command. For a snapshot-based
    /// `EventStore` see [`PostgresSnapshotStore`](struct.PostgresSnapshotStore.html).
    ///
    /// ```ignore
    /// # use postgres_es::PostgresStore;
    /// # use cqrs_es::CqrsFramework;
    /// let store = PostgresStore::<MyAggregate>::new(pool);
    /// let cqrs = CqrsFramework::new(store, vec![]);
    /// ```
    pub fn new(pool: Pool<Postgres>) -> Self {
        let repo = EventRepository::new(pool);
        PostgresStore {
            repo,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<A: Aggregate> EventStore<A> for PostgresStore<A> {
    type AC = PostgresStoreAggregateContext<A>;

    async fn load(&self, aggregate_id: &str) -> Vec<EventEnvelope<A>> {
        match self.repo.get_events(aggregate_id).await {
            Ok(val) => val,
            Err(_err) => {
                // TODO: improved error handling
                Default::default()
            }
        }
    }
    async fn load_aggregate(&self, aggregate_id: &str) -> PostgresStoreAggregateContext<A> {
        let committed_events = self.load(aggregate_id).await;
        let mut aggregate = A::default();
        let mut current_sequence = 0;
        for envelope in committed_events {
            current_sequence = envelope.sequence;
            let event = envelope.payload;
            aggregate.apply(event);
        }
        PostgresStoreAggregateContext {
            aggregate_id: aggregate_id.to_string(),
            aggregate,
            current_sequence,
        }
    }

    async fn commit(
        &self,
        events: Vec<A::Event>,
        context: PostgresStoreAggregateContext<A>,
        metadata: HashMap<String, String>,
    ) -> Result<Vec<EventEnvelope<A>>, AggregateError> {
        let aggregate_id = context.aggregate_id.as_str();
        let current_sequence = context.current_sequence;
        let wrapped_events = self.wrap_events(aggregate_id, current_sequence, events, metadata);
        self.repo.insert_events(wrapped_events.clone()).await?;
        Ok(wrapped_events)
    }
}

/// Holds context for a pure event store implementation for PostgresStore
pub struct PostgresStoreAggregateContext<A: Aggregate> {
    /// The aggregate ID of the aggregate instance that has been loaded.
    pub aggregate_id: String,
    /// The current state of the aggregate instance.
    pub aggregate: A,
    /// The last committed event sequence number for this aggregate instance.
    pub current_sequence: usize,
}

impl<A: Aggregate> AggregateContext<A> for PostgresStoreAggregateContext<A> {
    fn aggregate(&self) -> &A {
        &self.aggregate
    }
}

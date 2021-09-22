use std::collections::HashMap;
use std::marker::PhantomData;
use async_trait::async_trait;

use crate::connection::Connection;
use cqrs_es::{Aggregate, AggregateContext, AggregateError, EventEnvelope, EventStore};

/// Storage engine using an Postgres backing. This is the only persistent store currently
/// provided.
pub struct PostgresStore<A: Aggregate + Send + Sync> {
    conn: Connection,
    _phantom: PhantomData<A>,
}

impl<A: Aggregate> PostgresStore<A> {
    /// Creates a new `PostgresStore` from the provided database connection.
    pub fn new(conn: Connection) -> Self {
        PostgresStore {
            conn,
            _phantom: PhantomData,
        }
    }
}

static INSERT_EVENT: &str =
    "INSERT INTO events (aggregate_type, aggregate_id, sequence, payload, metadata)
                               VALUES ($1, $2, $3, $4, $5)";
static SELECT_EVENTS: &str = "SELECT aggregate_type, aggregate_id, sequence, payload, metadata
                                FROM events
                                WHERE aggregate_type = $1 AND aggregate_id = $2 ORDER BY sequence";

#[async_trait]
impl<A: Aggregate> EventStore<A, PostgresStoreAggregateContext<A>> for PostgresStore<A> {
    async fn load(&self, aggregate_id: &str) -> Vec<EventEnvelope<A>> {
        let agg_type = A::aggregate_type();
        let id = aggregate_id.to_string();
        let mut result = Vec::new();
        let mut conn = self.conn.conn();
        match conn.query(SELECT_EVENTS, &[&agg_type, &id]) {
            Ok(rows) => {
                for row in rows.iter() {
                    let aggregate_type: String = row.get("aggregate_type");
                    let aggregate_id: String = row.get("aggregate_id");
                    let s: i64 = row.get("sequence");
                    let sequence = s as usize;
                    let payload: A::Event = match serde_json::from_str(row.get("payload")) {
                        Ok(payload) => payload,
                        Err(err) => {
                            panic!("bad payload found in events table for aggregate id {} with error: {}", &id, err);
                        }
                    };
                    let event = EventEnvelope::new(aggregate_id, sequence, aggregate_type, payload);
                    result.push(event);
                }
            }
            Err(e) => {
                println!("{:?}", e);
            }
        }
        result
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
        let mut conn = self.conn.conn();
        let trans = match conn.transaction() {
            Ok(t) => t,
            Err(err) => {
                return Err(AggregateError::TechnicalError(err.to_string()));
            }
        };
        for event in &wrapped_events {
            let agg_type = event.aggregate_type.clone();
            let id = context.aggregate_id.clone();
            let sequence = event.sequence as i64;
            let payload = match serde_json::to_string(&event.payload) {
                Ok(payload) => payload,
                Err(err) => {
                    panic!(
                        "bad payload found in events table for aggregate id {} with error: {}",
                        &id, err
                    );
                }
            };
            let metadata = match serde_json::to_string(&event.metadata) {
                Ok(metadata) => metadata,
                Err(err) => {
                    panic!(
                        "bad metadata found in events table for aggregate id {} with error: {}",
                        &id, err
                    );
                }
            };
            let mut conn = self.conn.conn();
            match conn.execute(
                INSERT_EVENT,
                &[&agg_type, &id, &sequence, &payload, &metadata],
            ) {
                Ok(_) => {}
                Err(err) => {
                    match err.code() {
                        None => {}
                        Some(state) => {
                            if state.code() == "23505" {
                                return Err(AggregateError::TechnicalError(
                                    "optimistic lock error".to_string(),
                                ));
                            }
                        }
                    }
                    panic!("unable to insert event table for aggregate id {} with error: {}\n  and payload: {}", &id, err, &payload);
                }
            };
        }
        match trans.commit() {
            Ok(_) => Ok(wrapped_events),
            Err(err) => Err(AggregateError::TechnicalError(err.to_string())),
        }
    }
}

/// Holds context for a pure event store implementation for MemStore
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

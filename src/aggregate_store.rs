use std::collections::HashMap;
use std::marker::PhantomData;

use crate::connection::Connection;
use cqrs_es::{Aggregate, AggregateContext, AggregateError, EventEnvelope, EventStore};

/// Storage engine using an Postgres backing and relying on a serialization of the aggregate rather
/// than individual events. This is similar to the "snapshot strategy" seen in many CQRS
/// frameworks.
pub struct PostgresSnapshotStore<A: Aggregate> {
    conn: Connection,
    _phantom: PhantomData<A>,
}

impl<A: Aggregate> PostgresSnapshotStore<A> {
    /// Creates a new `PostgresSnapshotStore` from the provided database connection.
    pub fn new(conn: Connection) -> Self {
        PostgresSnapshotStore {
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

static INSERT_SNAPSHOT: &str =
    "INSERT INTO snapshots (aggregate_type, aggregate_id, last_sequence, payload)
                               VALUES ($1, $2, $3, $4)";
static UPDATE_SNAPSHOT: &str = "UPDATE snapshots
                               SET last_sequence= $3 , payload= $4
                               WHERE aggregate_type= $1 AND aggregate_id= $2";
static SELECT_SNAPSHOT: &str = "SELECT aggregate_type, aggregate_id, last_sequence, payload
                                FROM snapshots
                                WHERE aggregate_type = $1 AND aggregate_id = $2";

impl<A: Aggregate> EventStore<A, PostgresSnapshotStoreAggregateContext<A>>
    for PostgresSnapshotStore<A>
{
    fn load(&self, aggregate_id: &str) -> Vec<EventEnvelope<A>> {
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
                panic!("{:?}", e);
            }
        }
        result
    }
    fn load_aggregate(&self, aggregate_id: &str) -> PostgresSnapshotStoreAggregateContext<A> {
        let agg_type = A::aggregate_type();
        let mut conn = self.conn.conn();
        match conn.query(SELECT_SNAPSHOT, &[&agg_type, &aggregate_id.to_string()]) {
            Ok(rows) => match rows.get(0) {
                None => {
                    let current_sequence = 0;
                    PostgresSnapshotStoreAggregateContext {
                        aggregate_id: aggregate_id.to_string(),
                        aggregate: A::default(),
                        current_sequence,
                    }
                }
                Some(row) => {
                    let s: i64 = row.get("last_sequence");
                    let val: String = row.get("payload");
                    let aggregate = serde_json::from_str(&val).unwrap();
                    PostgresSnapshotStoreAggregateContext {
                        aggregate_id: aggregate_id.to_string(),
                        aggregate,
                        current_sequence: s as usize,
                    }
                }
            },
            Err(e) => {
                panic!("{:?}", e);
            }
        }
    }

    fn commit(
        &self,
        events: Vec<A::Event>,
        context: PostgresSnapshotStoreAggregateContext<A>,
        metadata: HashMap<String, String>,
    ) -> Result<Vec<EventEnvelope<A>>, AggregateError> {
        let mut updated_aggregate = context.aggregate_copy();
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
        let mut last_sequence = current_sequence as i64;
        for event in wrapped_events.clone() {
            let agg_type = event.aggregate_type.clone();
            let id = context.aggregate_id.clone();
            let sequence = event.sequence as i64;
            last_sequence = sequence;
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
            updated_aggregate.apply(event.payload);
        }

        let agg_type = A::aggregate_type();
        let aggregate_payload = match serde_json::to_string(&updated_aggregate) {
            Ok(val) => val,
            Err(err) => {
                panic!(
                    "bad metadata found in events table for aggregate id {} with error: {}",
                    &aggregate_id, err
                );
            }
        };
        let mut conn = self.conn.conn();
        if context.current_sequence == 0 {
            match conn.execute(
                INSERT_SNAPSHOT,
                &[&agg_type, &aggregate_id, &last_sequence, &aggregate_payload],
            ) {
                Ok(_) => {}
                Err(err) => {
                    panic!("unable to insert snapshot for aggregate id {} with error: {}\n  and payload: {}", &aggregate_id, err, &aggregate_payload);
                }
            };
        } else {
            match conn.execute(
                UPDATE_SNAPSHOT,
                &[&agg_type, &aggregate_id, &last_sequence, &aggregate_payload],
            ) {
                Ok(_) => {}
                Err(err) => {
                    panic!("unable to update snapshot for aggregate id {} with error: {}\n  and payload: {}", &aggregate_id, err, &aggregate_payload);
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
pub struct PostgresSnapshotStoreAggregateContext<A>
where
    A: Aggregate,
{
    /// The aggregate ID of the aggregate instance that has been loaded.
    pub aggregate_id: String,
    /// The current state of the aggregate instance.
    aggregate: A,
    /// The last committed event sequence number for this aggregate instance.
    pub current_sequence: usize,
}

impl<A> AggregateContext<A> for PostgresSnapshotStoreAggregateContext<A>
where
    A: Aggregate,
{
    fn aggregate(&self) -> &A {
        &self.aggregate
    }
}

impl<A> PostgresSnapshotStoreAggregateContext<A>
where
    A: Aggregate,
{
    pub(crate) fn aggregate_copy(&self) -> A {
        let ser = serde_json::to_value(&self.aggregate).unwrap();
        serde_json::from_value(ser).unwrap()
    }
}

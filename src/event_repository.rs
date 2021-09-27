use std::marker::PhantomData;

use crate::error::PostgresAggregateError;
use cqrs_es::{Aggregate, DomainEvent, EventEnvelope};
use futures::TryStreamExt;
use sqlx::postgres::PgRow;
use sqlx::Row;
use sqlx::{Pool, Postgres, Transaction};
use std::collections::HashMap;

pub(crate) static INSERT_EVENT: &str =
    "INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata)
                               VALUES ($1, $2, $3, $4, $5, $6, $7)";
pub(crate) static SELECT_EVENTS: &str =
    "SELECT aggregate_type, aggregate_id, sequence, payload, metadata
                                FROM events
                                WHERE aggregate_type = $1 AND aggregate_id = $2 ORDER BY sequence";

pub(crate) struct EventRepository<A> {
    pool: Pool<Postgres>,
    _phantom: PhantomData<A>,
}

impl<A> EventRepository<A>
where
    A: Aggregate,
{
    pub(crate) fn new(pool: Pool<Postgres>) -> Self {
        Self {
            pool,
            _phantom: Default::default(),
        }
    }
    pub(crate) async fn get_events(
        &self,
        aggregate_id: &str,
    ) -> Result<Vec<EventEnvelope<A>>, PostgresAggregateError> {
        let mut rows = sqlx::query(SELECT_EVENTS)
            .bind(A::aggregate_type())
            .bind(&aggregate_id)
            .fetch(&self.pool);
        let mut result: Vec<EventEnvelope<A>> = Default::default();
        while let Some(row) = rows.try_next().await? {
            result.push(self.deser_event(row)?);
        }
        Ok(result)
    }
    pub(crate) async fn insert_events(
        &self,
        events: Vec<EventEnvelope<A>>,
    ) -> Result<(), PostgresAggregateError> {
        let mut tx: Transaction<Postgres> = sqlx::Acquire::begin(&self.pool).await?;
        for event in events {
            let event_type = event.payload.event_type();
            let event_version = event.payload.event_version();
            let payload = serde_json::to_value(&event.payload)?;
            let metadata = serde_json::to_value(&event.metadata)?;
            sqlx::query(INSERT_EVENT)
                .bind(A::aggregate_type())
                .bind(event.aggregate_id.as_str())
                .bind(event.sequence as u32)
                .bind(event_type)
                .bind(event_version)
                .bind(&payload)
                .bind(&metadata)
                .execute(&mut tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    fn deser_event(&self, row: PgRow) -> Result<EventEnvelope<A>, PostgresAggregateError> {
        let aggregate_type: String = row.get("aggregate_type");
        let aggregate_id: String = row.get("aggregate_id");
        let sequence = {
            let s: i64 = row.get("sequence");
            s as usize
        };
        let payload: A::Event = serde_json::from_value(row.get("payload"))?;
        let metadata: HashMap<String, String> = serde_json::from_value(row.get("metadata"))?;
        Ok(EventEnvelope::new_with_metadata(
            aggregate_id,
            sequence,
            aggregate_type,
            payload,
            metadata,
        ))
    }
}

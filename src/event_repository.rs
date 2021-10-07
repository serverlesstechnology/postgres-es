use futures::TryStreamExt;
use std::collections::HashMap;
use std::marker::PhantomData;

use async_trait::async_trait;
use cqrs_es::{Aggregate, DomainEvent, EventEnvelope};
use persist_es::{PersistedEventRepository, PersistenceError, SnapshotStoreAggregateContext};
use sqlx::postgres::PgRow;
use sqlx::{Pool, Postgres, Row, Transaction};

use crate::error::PostgresAggregateError;

static INSERT_EVENT: &str =
    "INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata)
                               VALUES ($1, $2, $3, $4, $5, $6, $7)";

static SELECT_EVENTS: &str = "SELECT aggregate_type, aggregate_id, sequence, payload, metadata
                                FROM events
                                WHERE aggregate_type = $1 
                                  AND aggregate_id = $2 ORDER BY sequence";

static INSERT_SNAPSHOT: &str =
    "INSERT INTO snapshots (aggregate_type, aggregate_id, last_sequence, current_snapshot, payload)
                               VALUES ($1, $2, $3, $4, $5)";
static UPDATE_SNAPSHOT: &str = "UPDATE snapshots
                               SET last_sequence= $3 , payload= $6, current_snapshot= $4
                               WHERE aggregate_type= $1 AND aggregate_id= $2 AND current_snapshot= $5";
static SELECT_SNAPSHOT: &str =
    "SELECT aggregate_type, aggregate_id, last_sequence, current_snapshot, payload
                                FROM snapshots
                                WHERE aggregate_type = $1 AND aggregate_id = $2";

/// A snapshot backed event repository for use in backing a `PersistedSnapshotStore`.
pub struct PostgresEventRepository<A> {
    pool: Pool<Postgres>,
    _phantom: PhantomData<A>,
}

#[async_trait]
impl<A> PersistedEventRepository<A> for PostgresEventRepository<A>
where
    A: Aggregate,
{
    async fn get_events(
        &self,
        aggregate_id: &str,
    ) -> Result<Vec<EventEnvelope<A>>, PersistenceError> {
        let query = SELECT_EVENTS;
        self.select_events(aggregate_id, query).await
    }

    async fn get_last_events(
        &self,
        aggregate_id: &str,
        number_events: usize,
    ) -> Result<Vec<EventEnvelope<A>>, PersistenceError> {
        let query = format!(
            "SELECT aggregate_type, aggregate_id, sequence, payload, metadata
                                FROM events
                                WHERE aggregate_type = $1 AND aggregate_id = $2
                                  AND sequence > (SELECT max(sequence)
                                                  FROM events
                                                  WHERE aggregate_type = $1
                                                    AND aggregate_id = $2) - {}
                                ORDER BY sequence",
            number_events
        );
        self.select_events(aggregate_id, &query).await
    }

    async fn get_snapshot(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<SnapshotStoreAggregateContext<A>>, PersistenceError> {
        let row: PgRow = match sqlx::query(SELECT_SNAPSHOT)
            .bind(A::aggregate_type())
            .bind(&aggregate_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(PostgresAggregateError::from)?
        {
            Some(row) => row,
            None => {
                return Ok(None);
            }
        };
        Ok(Some(self.deser_snapshot(row)?))
    }

    async fn persist(
        &self,
        events: &[EventEnvelope<A>],
        snapshot_update: Option<(String, A, usize)>,
    ) -> Result<(), PersistenceError> {
        match snapshot_update {
            None => {
                self.insert_events(events).await?;
            }
            Some((aggregate_id, aggregate, current_snapshot)) => {
                if current_snapshot == 1 {
                    self.insert(aggregate, aggregate_id, current_snapshot, events)
                        .await?;
                } else {
                    self.update(aggregate, aggregate_id, current_snapshot, events)
                        .await?;
                }
            }
        };
        Ok(())
    }
}

impl<A> PostgresEventRepository<A>
where
    A: Aggregate,
{
    async fn select_events(
        &self,
        aggregate_id: &str,
        query: &str,
    ) -> Result<Vec<EventEnvelope<A>>, PersistenceError> {
        let mut rows = sqlx::query(query)
            .bind(A::aggregate_type())
            .bind(aggregate_id)
            .fetch(&self.pool);
        let mut result: Vec<EventEnvelope<A>> = Default::default();
        while let Some(row) = rows
            .try_next()
            .await
            .map_err(PostgresAggregateError::from)?
        {
            result.push(self.deser_event(row)?);
        }
        Ok(result)
    }
}

impl<A> PostgresEventRepository<A>
where
    A: Aggregate,
{
    /// Creates a new `PostgresEventRepository` from the provided database connection
    /// used for backing a `PersistedSnapshotStore`.
    ///
    /// ```ignore
    /// let store = PostgresEventRepository::<MyAggregate>::new(pool);
    /// ```
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self {
            pool,
            _phantom: Default::default(),
        }
    }

    pub(crate) async fn insert_events(
        &self,
        events: &[EventEnvelope<A>],
    ) -> Result<(), PostgresAggregateError> {
        let mut tx: Transaction<Postgres> = sqlx::Acquire::begin(&self.pool).await?;
        PostgresEventRepository::<A>::persist_events(&mut tx, events).await?;
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn insert(
        &self,
        aggregate: A,
        aggregate_id: String,
        current_snapshot: usize,
        events: &[EventEnvelope<A>],
    ) -> Result<(), PostgresAggregateError> {
        let mut tx: Transaction<Postgres> = sqlx::Acquire::begin(&self.pool).await?;
        let current_sequence =
            PostgresEventRepository::<A>::persist_events(&mut tx, events).await?;
        let aggregate_payload = serde_json::to_value(&aggregate)?;
        sqlx::query(INSERT_SNAPSHOT)
            .bind(A::aggregate_type())
            .bind(aggregate_id.as_str())
            .bind(current_sequence as u32)
            .bind(current_snapshot as u32)
            .bind(&aggregate_payload)
            .execute(&mut tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn update(
        &self,
        aggregate: A,
        aggregate_id: String,
        current_snapshot: usize,
        events: &[EventEnvelope<A>],
    ) -> Result<(), PostgresAggregateError> {
        let mut tx: Transaction<Postgres> = sqlx::Acquire::begin(&self.pool).await?;
        let current_sequence =
            PostgresEventRepository::<A>::persist_events(&mut tx, events).await?;

        let aggregate_payload = serde_json::to_value(&aggregate)?;
        sqlx::query(UPDATE_SNAPSHOT)
            .bind(A::aggregate_type())
            .bind(aggregate_id.as_str())
            .bind(current_sequence as u32)
            .bind(current_snapshot as u32)
            .bind((current_snapshot - 1) as u32)
            .bind(&aggregate_payload)
            .execute(&mut tx)
            .await?;
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

    fn deser_snapshot(
        &self,
        row: PgRow,
    ) -> Result<SnapshotStoreAggregateContext<A>, PostgresAggregateError> {
        let aggregate_id = row.get("aggregate_id");
        let s: i64 = row.get("last_sequence");
        let current_sequence = s as usize;
        let s: i64 = row.get("current_snapshot");
        let current_snapshot = s as usize;
        let aggregate: A = serde_json::from_value(row.get("payload"))?;
        Ok(SnapshotStoreAggregateContext {
            aggregate_id,
            aggregate,
            current_sequence,
            current_snapshot,
        })
    }

    pub(crate) async fn persist_events(
        tx: &mut Transaction<'_, Postgres>,
        events: &[EventEnvelope<A>],
    ) -> Result<usize, PostgresAggregateError> {
        let mut current_sequence: usize = 0;
        for event in events {
            current_sequence = event.sequence;
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
                .execute(&mut *tx)
                .await?;
        }
        Ok(current_sequence)
    }
}

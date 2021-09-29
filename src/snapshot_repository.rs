use std::marker::PhantomData;

use async_trait::async_trait;
use cqrs_es::persist::{
    PersistedSnapshotEventRepository, PersistenceError, SnapshotStoreAggregateContext,
};
use cqrs_es::{Aggregate, EventEnvelope};
use sqlx::postgres::PgRow;
use sqlx::{Pool, Postgres, Row, Transaction};

use crate::error::PostgresAggregateError;
use crate::event_repository::PostgresEventRepository;

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
pub struct PostgresSnapshotRepository<A> {
    pool: Pool<Postgres>,
    _phantom: PhantomData<A>,
}

#[async_trait]
impl<A> PersistedSnapshotEventRepository<A> for PostgresSnapshotRepository<A>
where
    A: Aggregate,
{
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
        aggregate: A,
        aggregate_id: String,
        current_snapshot: usize,
        events: &[EventEnvelope<A>],
    ) -> Result<(), PersistenceError> {
        if current_snapshot == 1 {
            self.insert(aggregate, aggregate_id, current_snapshot, &events)
                .await?;
        } else {
            self.update(aggregate, aggregate_id, current_snapshot, &events)
                .await?;
        }
        Ok(())
    }
}
impl<A> PostgresSnapshotRepository<A>
where
    A: Aggregate,
{
    /// Creates a new `PostgresSnapshotRepository` from the provided database connection
    /// used for backing a `PersistedSnapshotStore`.
    ///
    /// ```ignore
    /// let store = PostgresSnapshotRepository::<MyAggregate>::new(pool);
    /// ```
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self {
            pool,
            _phantom: Default::default(),
        }
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
}

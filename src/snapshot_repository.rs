use std::marker::PhantomData;

use cqrs_es::Aggregate;
use sqlx::{Pool, Postgres, Row, Transaction};
use sqlx::postgres::PgRow;

use crate::error::PostgresAggregateError;
use crate::PostgresSnapshotStoreAggregateContext;

static INSERT_SNAPSHOT: &str =
    "INSERT INTO snapshots (aggregate_type, aggregate_id, last_sequence, current_snapshot, payload)
                               VALUES ($1, $2, $3, $4, $5)";
static UPDATE_SNAPSHOT: &str = "UPDATE snapshots
                               SET last_sequence= $3 , payload= $6, current_snapshot= $4
                               WHERE aggregate_type= $1 AND aggregate_id= $2 AND current_snapshot= $5";
static SELECT_SNAPSHOT: &str = "SELECT aggregate_type, aggregate_id, last_sequence, current_snapshot, payload
                                FROM snapshots
                                WHERE aggregate_type = $1 AND aggregate_id = $2";

pub struct SnapshotRepository<A> {
    pool: Pool<Postgres>,
    _phantom: PhantomData<A>,
}

impl<A> SnapshotRepository<A>
    where A: Aggregate
{
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool, _phantom: Default::default() }
    }
    pub async fn get_snapshot(&self, aggregate_id: &str) -> Result<Option<PostgresSnapshotStoreAggregateContext<A>>, PostgresAggregateError>
    {
        let row: PgRow = match sqlx::query(SELECT_SNAPSHOT)
            .bind(A::aggregate_type())
            .bind(&aggregate_id)
            .fetch_optional(&self.pool)
            .await? {
            Some(row) => row,
            None => { return Ok(None); }
        };
        Ok(Some(self.deser_snapshot(row)?))
    }

    pub async fn insert(&self, aggregate: A, aggregate_id: String, current_sequence: usize, current_snapshot: usize) -> Result<(), PostgresAggregateError> {
        let mut tx: Transaction<Postgres> = sqlx::Acquire::begin(&self.pool).await?;
        let aggregate_payload = serde_json::to_string(&aggregate)?;
        sqlx::query(INSERT_SNAPSHOT)
            .bind(A::aggregate_type())
            .bind(aggregate_id.as_str())
            .bind(current_sequence as u32)
            .bind(current_snapshot as u32)
            .bind(&aggregate_payload)
            .execute(&mut tx).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn update(&self, aggregate: A, aggregate_id: String, current_sequence: usize, current_snapshot: usize) -> Result<(), PostgresAggregateError> {
        let mut tx: Transaction<Postgres> = sqlx::Acquire::begin(&self.pool).await?;
        let aggregate_payload = serde_json::to_string(&aggregate)?;
        sqlx::query(UPDATE_SNAPSHOT)
            .bind(A::aggregate_type())
            .bind(aggregate_id.as_str())
            .bind(current_sequence as u32)
            .bind(current_snapshot as u32)
            .bind((current_snapshot-1) as u32)
            .bind(&aggregate_payload)
            .execute(&mut tx).await?;
        tx.commit().await?;
        Ok(())
    }

    fn deser_snapshot(&self, row: PgRow) -> Result<PostgresSnapshotStoreAggregateContext<A>, PostgresAggregateError> {
        let aggregate_id = row.get("aggregate_id");
        let s: i64 = row.get("last_sequence");
        let current_sequence = s as usize;
        let s: i64 = row.get("current_snapshot");
        let current_snapshot = s as usize;
        let payload: String = row.get("payload");
        let aggregate: A = serde_json::from_str(&payload)?;
        Ok(PostgresSnapshotStoreAggregateContext {
            aggregate_id,
            aggregate,
            current_sequence,
            current_snapshot
        })
    }
}
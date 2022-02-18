use futures::TryStreamExt;

use async_trait::async_trait;
use cqrs_es::Aggregate;
use persist_es::{PersistedEventRepository, PersistenceError, SerializedEvent, SerializedSnapshot};
use serde_json::Value;
use sqlx::postgres::PgRow;
use sqlx::{Pool, Postgres, Row, Transaction};

use crate::error::PostgresAggregateError;

static INSERT_EVENT: &str =
    "INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata)
                               VALUES ($1, $2, $3, $4, $5, $6, $7)";

static SELECT_EVENTS: &str =
    "SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata
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
pub struct PostgresEventRepository {
    pool: Pool<Postgres>,
}

#[async_trait]
impl PersistedEventRepository for PostgresEventRepository {
    async fn get_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        let query = SELECT_EVENTS;
        self.select_events::<A>(aggregate_id, query).await
    }

    async fn get_last_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
        number_events: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
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
        self.select_events::<A>(aggregate_id, &query).await
    }

    async fn get_snapshot<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<SerializedSnapshot>, PersistenceError> {
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

    async fn persist<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<(String, Value, usize)>,
    ) -> Result<(), PersistenceError> {
        match snapshot_update {
            None => {
                self.insert_events::<A>(events).await?;
            }
            Some((aggregate_id, aggregate, current_snapshot)) => {
                if current_snapshot == 1 {
                    self.insert::<A>(aggregate, aggregate_id, current_snapshot, events)
                        .await?;
                } else {
                    self.update::<A>(aggregate, aggregate_id, current_snapshot, events)
                        .await?;
                }
            }
        };
        Ok(())
    }
}

impl PostgresEventRepository {
    async fn select_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
        query: &str,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        let mut rows = sqlx::query(query)
            .bind(A::aggregate_type())
            .bind(aggregate_id)
            .fetch(&self.pool);
        let mut result: Vec<SerializedEvent> = Default::default();
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

impl PostgresEventRepository {
    /// Creates a new `PostgresEventRepository` from the provided database connection
    /// used for backing a `PersistedSnapshotStore`.
    ///
    /// ```ignore
    /// let store = PostgresEventRepository::<MyAggregate>::new(pool);
    /// ```
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    pub(crate) async fn insert_events<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
    ) -> Result<(), PostgresAggregateError> {
        let mut tx: Transaction<Postgres> = sqlx::Acquire::begin(&self.pool).await?;
        self.persist_events::<A>(&mut tx, events).await?;
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn insert<A: Aggregate>(
        &self,
        aggregate_payload: Value,
        aggregate_id: String,
        current_snapshot: usize,
        events: &[SerializedEvent],
    ) -> Result<(), PostgresAggregateError> {
        let mut tx: Transaction<Postgres> = sqlx::Acquire::begin(&self.pool).await?;
        let current_sequence = self.persist_events::<A>(&mut tx, events).await?;
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

    pub(crate) async fn update<A: Aggregate>(
        &self,
        aggregate: Value,
        aggregate_id: String,
        current_snapshot: usize,
        events: &[SerializedEvent],
    ) -> Result<(), PostgresAggregateError> {
        let mut tx: Transaction<Postgres> = sqlx::Acquire::begin(&self.pool).await?;
        let current_sequence = self.persist_events::<A>(&mut tx, events).await?;

        let aggregate_payload = serde_json::to_value(&aggregate)?;
        let result = sqlx::query(UPDATE_SNAPSHOT)
            .bind(A::aggregate_type())
            .bind(aggregate_id.as_str())
            .bind(current_sequence as u32)
            .bind(current_snapshot as u32)
            .bind((current_snapshot - 1) as u32)
            .bind(&aggregate_payload)
            .execute(&mut tx)
            .await?;
        tx.commit().await?;
        match result.rows_affected() {
            1 => Ok(()),
            _ => Err(PostgresAggregateError::OptimisticLock),
        }
    }

    fn deser_event(&self, row: PgRow) -> Result<SerializedEvent, PostgresAggregateError> {
        let aggregate_type: String = row.get("aggregate_type");
        let aggregate_id: String = row.get("aggregate_id");
        let sequence = {
            let s: i64 = row.get("sequence");
            s as usize
        };
        let event_type: String = row.get("event_type");
        let event_version: String = row.get("event_version");
        let payload: Value = row.get("payload");
        let metadata: Value = row.get("metadata");
        Ok(SerializedEvent::new(
            aggregate_id,
            sequence,
            aggregate_type,
            event_type,
            event_version,
            payload,
            metadata,
        ))
    }

    fn deser_snapshot(&self, row: PgRow) -> Result<SerializedSnapshot, PostgresAggregateError> {
        let aggregate_id = row.get("aggregate_id");
        let s: i64 = row.get("last_sequence");
        let current_sequence = s as usize;
        let s: i64 = row.get("current_snapshot");
        let current_snapshot = s as usize;
        let aggregate: Value = row.get("payload");
        Ok(SerializedSnapshot {
            aggregate_id,
            aggregate,
            current_sequence,
            current_snapshot,
        })
    }

    pub(crate) async fn persist_events<A: Aggregate>(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        events: &[SerializedEvent],
    ) -> Result<usize, PostgresAggregateError> {
        let mut current_sequence: usize = 0;
        for event in events {
            current_sequence = event.sequence;
            let event_type = &event.event_type;
            let event_version = &event.event_version;
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

#[cfg(test)]
mod test {
    use crate::error::PostgresAggregateError;
    use crate::testing::tests::{
        new_test_event_store, new_test_metadata, new_test_snapshot_store, snapshot_context,
        test_event_envelope, Created, SomethingElse, TestAggregate, TestEvent, Tested,
        TEST_CONNECTION_STRING,
    };
    use crate::{default_postgress_pool, PostgresEventRepository};
    use cqrs_es::EventStore;
    use persist_es::PersistedEventRepository;

    #[tokio::test]
    async fn commit_and_load_events() {
        let pool = default_postgress_pool(TEST_CONNECTION_STRING).await;
        let event_store = new_test_event_store(pool).await;
        let id = uuid::Uuid::new_v4().to_string();
        assert_eq!(0, event_store.load(id.as_str()).await.len());
        let context = event_store.load_aggregate(id.as_str()).await;

        event_store
            .commit(
                vec![
                    TestEvent::Created(Created {
                        id: "test_event_A".to_string(),
                    }),
                    TestEvent::Tested(Tested {
                        test_name: "test A".to_string(),
                    }),
                ],
                context,
                new_test_metadata(),
            )
            .await
            .unwrap();

        assert_eq!(2, event_store.load(id.as_str()).await.len());
        let context = event_store.load_aggregate(id.as_str()).await;

        event_store
            .commit(
                vec![TestEvent::Tested(Tested {
                    test_name: "test B".to_string(),
                })],
                context,
                new_test_metadata(),
            )
            .await
            .unwrap();
        assert_eq!(3, event_store.load(id.as_str()).await.len());
    }

    #[tokio::test]
    async fn commit_and_load_events_snapshot_store() {
        let pool = default_postgress_pool(TEST_CONNECTION_STRING).await;
        let event_store = new_test_snapshot_store(pool).await;
        let id = uuid::Uuid::new_v4().to_string();
        assert_eq!(0, event_store.load(id.as_str()).await.len());
        let context = event_store.load_aggregate(id.as_str()).await;

        event_store
            .commit(
                vec![
                    TestEvent::Created(Created {
                        id: "test_event_A".to_string(),
                    }),
                    TestEvent::Tested(Tested {
                        test_name: "test A".to_string(),
                    }),
                ],
                context,
                new_test_metadata(),
            )
            .await
            .unwrap();

        assert_eq!(2, event_store.load(id.as_str()).await.len());
        let context = event_store.load_aggregate(id.as_str()).await;

        event_store
            .commit(
                vec![TestEvent::Tested(Tested {
                    test_name: "test B".to_string(),
                })],
                context,
                new_test_metadata(),
            )
            .await
            .unwrap();
        assert_eq!(3, event_store.load(id.as_str()).await.len());
    }

    #[tokio::test]
    async fn event_repositories() {
        let pool = default_postgress_pool(TEST_CONNECTION_STRING).await;
        let id = uuid::Uuid::new_v4().to_string();
        let event_repo: PostgresEventRepository = PostgresEventRepository::new(pool.clone());
        let events = event_repo.get_events::<TestAggregate>(&id).await.unwrap();
        assert!(events.is_empty());

        event_repo
            .insert_events::<TestAggregate>(&[
                test_event_envelope(&id, 1, TestEvent::Created(Created { id: id.clone() })),
                test_event_envelope(
                    &id,
                    2,
                    TestEvent::Tested(Tested {
                        test_name: "a test was run".to_string(),
                    }),
                ),
            ])
            .await
            .unwrap();
        let events = event_repo.get_events::<TestAggregate>(&id).await.unwrap();
        assert_eq!(2, events.len());
        events.iter().for_each(|e| assert_eq!(&id, &e.aggregate_id));

        // Optimistic lock error
        let result = event_repo
            .insert_events::<TestAggregate>(&[
                test_event_envelope(
                    &id,
                    3,
                    TestEvent::SomethingElse(SomethingElse {
                        description: "this should not persist".to_string(),
                    }),
                ),
                test_event_envelope(
                    &id,
                    2,
                    TestEvent::SomethingElse(SomethingElse {
                        description: "bad sequence number".to_string(),
                    }),
                ),
            ])
            .await
            .unwrap_err();
        match result {
            PostgresAggregateError::OptimisticLock => {}
            _ => panic!("invalid error result found during insert: {}", result),
        };

        let events = event_repo.get_events::<TestAggregate>(&id).await.unwrap();
        assert_eq!(2, events.len());
    }

    #[tokio::test]
    async fn snapshot_repositories() {
        let pool = default_postgress_pool(TEST_CONNECTION_STRING).await;
        let id = uuid::Uuid::new_v4().to_string();
        let repo: PostgresEventRepository = PostgresEventRepository::new(pool.clone());
        let snapshot = repo.get_snapshot::<TestAggregate>(&id).await.unwrap();
        assert_eq!(None, snapshot);

        let test_description = "some test snapshot here".to_string();
        let test_tests = vec!["testA".to_string(), "testB".to_string()];
        repo.insert::<TestAggregate>(
            serde_json::to_value(TestAggregate {
                id: id.clone(),
                description: test_description.clone(),
                tests: test_tests.clone(),
            })
            .unwrap(),
            id.clone(),
            1,
            &vec![],
        )
        .await
        .unwrap();

        let snapshot = repo.get_snapshot::<TestAggregate>(&id).await.unwrap();
        assert_eq!(
            Some(snapshot_context(
                id.clone(),
                0,
                1,
                serde_json::to_value(TestAggregate {
                    id: id.clone(),
                    description: test_description.clone(),
                    tests: test_tests.clone(),
                })
                .unwrap()
            )),
            snapshot
        );

        // sequence iterated, does update
        repo.update::<TestAggregate>(
            serde_json::to_value(TestAggregate {
                id: id.clone(),
                description: "a test description that should be saved".to_string(),
                tests: test_tests.clone(),
            })
            .unwrap(),
            id.clone(),
            2,
            &vec![],
        )
        .await
        .unwrap();

        let snapshot = repo.get_snapshot::<TestAggregate>(&id).await.unwrap();
        assert_eq!(
            Some(snapshot_context(
                id.clone(),
                0,
                2,
                serde_json::to_value(TestAggregate {
                    id: id.clone(),
                    description: "a test description that should be saved".to_string(),
                    tests: test_tests.clone(),
                })
                .unwrap()
            )),
            snapshot
        );

        // sequence out of order or not iterated, does not update
        let result = repo
            .update::<TestAggregate>(
                serde_json::to_value(TestAggregate {
                    id: id.clone(),
                    description: "a test description that should not be saved".to_string(),
                    tests: test_tests.clone(),
                })
                .unwrap(),
                id.clone(),
                2,
                &vec![],
            )
            .await
            .unwrap_err();
        match result {
            PostgresAggregateError::OptimisticLock => {}
            _ => panic!("invalid error result found during insert: {}", result),
        };

        let snapshot = repo.get_snapshot::<TestAggregate>(&id).await.unwrap();
        assert_eq!(
            Some(snapshot_context(
                id.clone(),
                0,
                2,
                serde_json::to_value(TestAggregate {
                    id: id.clone(),
                    description: "a test description that should be saved".to_string(),
                    tests: test_tests.clone(),
                })
                .unwrap()
            )),
            snapshot
        );
    }
}

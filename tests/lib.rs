
use cqrs_es::{Aggregate, AggregateError, DomainEvent, EventEnvelope, EventStore, Query};
use serde::{Deserialize, Serialize};

use postgres_es::{GenericQueryRepository};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct TestAggregate {
    id: String,
    description: String,
    tests: Vec<String>,
}

impl Aggregate for TestAggregate {
    type Command = TestCommand;
    type Event = TestEvent;

    fn aggregate_type() -> &'static str {
        "TestAggregate"
    }

    fn handle(&self, command: Self::Command) -> Result<Vec<Self::Event>, AggregateError> {
        match command {
            TestCommand::CreateTest(command) => {
                let event = TestEvent::Created(Created {
                    id: command.id.to_string(),
                });
                Ok(vec![event])
            }
            TestCommand::ConfirmTest(command) => {
                for test in &self.tests {
                    if test == &command.test_name {
                        return Err(AggregateError::new("test already performed"));
                    }
                }
                let event = TestEvent::Tested(Tested {
                    test_name: command.test_name,
                });
                Ok(vec![event])
            }
            TestCommand::DoSomethingElse(command) => {
                let event = TestEvent::SomethingElse(SomethingElse {
                    description: command.description.clone(),
                });
                Ok(vec![event])
            }
        }
    }

    fn apply(&mut self, e: Self::Event) {
        match e {
            TestEvent::Created(e) => {
                self.id = e.id.clone();
            }
            TestEvent::Tested(e) => self.tests.push(e.test_name.clone()),
            TestEvent::SomethingElse(e) => {
                self.description = e.description.clone();
            }
        }
    }
}

impl Default for TestAggregate {
    fn default() -> Self {
        TestAggregate {
            id: "".to_string(),
            description: "".to_string(),
            tests: Vec::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum TestEvent {
    Created(Created),
    Tested(Tested),
    SomethingElse(SomethingElse),
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Created {
    pub id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Tested {
    pub test_name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct SomethingElse {
    pub description: String,
}

impl DomainEvent for TestEvent {}

pub enum TestCommand {
    CreateTest(CreateTest),
    ConfirmTest(ConfirmTest),
    DoSomethingElse(DoSomethingElse),
}

pub struct CreateTest {
    pub id: String,
}

pub struct ConfirmTest {
    pub test_name: String,
}

pub struct DoSomethingElse {
    pub description: String,
}

type TestQueryRepository = GenericQueryRepository<TestQuery,TestAggregate>;

#[derive(Debug,Default, Serialize,Deserialize)]
struct TestQuery {
    events: Vec<TestEvent>,
}

impl Query<TestAggregate> for TestQuery {
    fn update(&mut self, event: &EventEnvelope<TestAggregate>) {
        self.events.push(event.payload.clone());
    }
}

pub type TestEventEnvelope = EventEnvelope<TestAggregate>;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::{Map, Value};
    use sqlx::{Pool, Postgres};
    use sqlx::postgres::PgPoolOptions;
    use static_assertions::assert_impl_all;

    use postgres_es::{EventRepository, postgres_cqrs, PostgresSnapshotStore, PostgresSnapshotStoreAggregateContext, PostgresStore, PostgresStoreAggregateContext, SnapshotRepository};

    use super::*;

    assert_impl_all!(rdbmsstore; PostgresStore::<TestAggregate>, EventStore::<TestAggregate, PostgresStoreAggregateContext<TestAggregate>>);

    const CONNECTION_STRING: &str = "postgresql://test_user:test_pass@localhost:5432/test";

    async fn db_pool(connection_string: &str) -> Pool<Postgres> {
        PgPoolOptions::new()
            .max_connections(5)
            .connect(connection_string)
            .await
            .expect("unable to connect to database")
    }

    async fn test_event_repo(pool: Pool<Postgres>) -> EventRepository<TestAggregate> {
        EventRepository::new(pool)
    }

    async fn test_snapshot_repo(pool: Pool<Postgres>) -> SnapshotRepository<TestAggregate> {
        SnapshotRepository::new(pool)
    }

    async fn test_store(pool: Pool<Postgres>) -> PostgresStore<TestAggregate> {
        PostgresStore::<TestAggregate>::new(test_event_repo(pool).await)
    }

    async fn test_snapshot_store(pool: Pool<Postgres>) -> PostgresSnapshotStore<TestAggregate> {
        PostgresSnapshotStore::<TestAggregate>::new(test_snapshot_repo(pool.clone()).await, test_event_repo(pool).await)
    }

    fn test_metadata() -> HashMap<String, String> {
        let now = "2021-03-18T12:32:45.930Z".to_string();
        let mut metadata = HashMap::new();
        metadata.insert("time".to_string(), now);
        metadata
    }

    #[tokio::test]
    async fn test_valid_cqrs_framework() {
        let pool = db_pool(CONNECTION_STRING).await;
        let query = TestQueryRepository::new("test_query", pool.clone());
        let _ps = postgres_cqrs(test_event_repo(pool.clone()).await, vec![Box::new(query)]);
    }

    #[tokio::test]
    async fn query() {
        let pool = db_pool(CONNECTION_STRING).await;
        let query = TestQueryRepository::new("test_query", pool.clone());
        let id = uuid::Uuid::new_v4().to_string();
        query.apply_events(&id, &vec![
            EventEnvelope {
                aggregate_id: id.clone(),
                sequence: 1,
                aggregate_type: TestAggregate::aggregate_type().to_string(),
                payload: TestEvent::Created(Created { id: id.clone() }),
                metadata: Default::default(),
            },
            EventEnvelope {
                aggregate_id: id.clone(),
                sequence: 2,
                aggregate_type: TestAggregate::aggregate_type().to_string(),
                payload: TestEvent::Tested(Tested { test_name: "a test was run".to_string() }),
                metadata: Default::default(),
            },
        ]).await.unwrap();
        let result = query.load(id).await.unwrap();
        assert_eq!(2, result.events.len())
    }

    #[tokio::test]
    async fn commit_and_load_events() {
        let pool = db_pool(CONNECTION_STRING).await;
        let event_store = test_store(pool).await;
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
                test_metadata(),
            ).await
            .unwrap();

        assert_eq!(2, event_store.load(id.as_str()).await.len());
        let context = event_store.load_aggregate(id.as_str()).await;

        event_store
            .commit(
                vec![TestEvent::Tested(Tested {
                    test_name: "test B".to_string(),
                })],
                context,
                test_metadata(),
            ).await
            .unwrap();
        assert_eq!(3, event_store.load(id.as_str()).await.len());
    }

    #[tokio::test]
    async fn commit_and_load_events_snapshot_store() {
        let pool = db_pool(CONNECTION_STRING).await;
        let event_store = test_snapshot_store(pool).await;
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
                test_metadata(),
            ).await
            .unwrap();

        assert_eq!(2, event_store.load(id.as_str()).await.len());
        let context = event_store.load_aggregate(id.as_str()).await;

        event_store
            .commit(
                vec![TestEvent::Tested(Tested {
                    test_name: "test B".to_string(),
                })],
                context,
                test_metadata(),
            ).await
            .unwrap();
        assert_eq!(3, event_store.load(id.as_str()).await.len());
    }

    #[test]
    fn test_event_breakout_type() {
        let event = TestEvent::Created(Created {
            id: "test_event_A".to_string(),
        });

        let (event_type, value) = serialize_event::<TestAggregate>(&event);
        println!("{} - {}", &event_type, &value);
        let deser: TestEvent = deserialize_event::<TestAggregate>(event_type.as_str(), value);
        assert_eq!(deser, event);
    }

    fn serialize_event<A: Aggregate>(event: &A::Event) -> (String, Value) {
        let val = serde_json::to_value(event).unwrap();
        match &val {
            Value::Object(object) => {
                for key in object.keys() {
                    let value = object.get(key).unwrap();
                    return (key.to_string(), value.clone());
                }
                panic!("{:?} not a domain event", val);
            }
            _ => {
                panic!("{:?} not an object", val);
            }
        }
    }

    fn deserialize_event<A: Aggregate>(event_type: &str, value: Value) -> A::Event {
        let mut new_val_map = Map::with_capacity(1);
        new_val_map.insert(event_type.to_string(), value);
        let new_event_val = Value::Object(new_val_map);
        serde_json::from_value(new_event_val).unwrap()
    }

    #[tokio::test]
    async fn event_repositories() {
        let pool = db_pool("postgresql://test_user:test_pass@localhost:5432/test").await;
        let id = uuid::Uuid::new_v4().to_string();
        let event_repo: EventRepository<TestAggregate> = EventRepository::new(pool.clone());
        let events = event_repo.get_events(&id).await.unwrap();
        assert!(events.is_empty());

        event_repo.insert_events(vec![
            EventEnvelope {
                aggregate_id: id.clone(),
                sequence: 1,
                aggregate_type: TestAggregate::aggregate_type().to_string(),
                payload: TestEvent::Created(Created { id: id.clone() }),
                metadata: Default::default(),
            },
            EventEnvelope {
                aggregate_id: id.clone(),
                sequence: 2,
                aggregate_type: TestAggregate::aggregate_type().to_string(),
                payload: TestEvent::Tested(Tested { test_name: "a test was run".to_string() }),
                metadata: Default::default(),
            },
        ]).await.unwrap();
        let events = event_repo.get_events(&id).await.unwrap();
        assert_eq!(2, events.len());
        events.iter().for_each(|e| assert_eq!(&id, &e.aggregate_id));

        event_repo.insert_events(vec![
            EventEnvelope {
                aggregate_id: id.clone(),
                sequence: 3,
                aggregate_type: TestAggregate::aggregate_type().to_string(),
                payload: TestEvent::SomethingElse(SomethingElse { description: "this should not persist".to_string() }),
                metadata: Default::default(),
            },
            EventEnvelope {
                aggregate_id: id.clone(),
                sequence: 2,
                aggregate_type: TestAggregate::aggregate_type().to_string(),
                payload: TestEvent::SomethingElse(SomethingElse { description: "bad sequence number".to_string() }),
                metadata: Default::default(),
            },
        ]).await.unwrap_err();
        let events = event_repo.get_events(&id).await.unwrap();
        assert_eq!(2, events.len());
    }

    #[tokio::test]
    async fn snapshot_repositories() {
        let pool = db_pool("postgresql://test_user:test_pass@localhost:5432/test").await;
        let id = uuid::Uuid::new_v4().to_string();
        let repo: SnapshotRepository<TestAggregate> = SnapshotRepository::new(pool.clone());
        let snapshot = repo.get_snapshot(&id).await.unwrap();
        assert_eq!(None, snapshot);

        let test_description = "some test snapshot here".to_string();
        let test_tests = vec!["testA".to_string(), "testB".to_string()];
        repo.insert(TestAggregate {
            id: id.clone(),
            description: test_description.clone(),
            tests: test_tests.clone(),
        }, id.clone(), 1, &vec![]).await.unwrap();
        let snapshot = repo.get_snapshot(&id).await.unwrap();
        assert_eq!(Some(PostgresSnapshotStoreAggregateContext::new(id.clone(), 0, 1, TestAggregate {
            id: id.clone(),
            description: test_description.clone(),
            tests: test_tests.clone(),
        })), snapshot);

        // sequence iterated, does update
        repo.update(TestAggregate {
            id: id.clone(),
            description: "a test description that should be saved".to_string(),
            tests: test_tests.clone(),
        }, id.clone(), 2, &vec![]).await.unwrap();
        let snapshot = repo.get_snapshot(&id).await.unwrap();
        assert_eq!(Some(PostgresSnapshotStoreAggregateContext::new(id.clone(), 0, 2, TestAggregate {
            id: id.clone(),
            description: "a test description that should be saved".to_string(),
            tests: test_tests.clone(),
        })), snapshot);

        // sequence out of order or not iterated, does not update
        repo.update(TestAggregate {
            id: id.clone(),
            description: "a test description that should not be saved".to_string(),
            tests: test_tests.clone(),
        }, id.clone(), 2, &vec![]).await.unwrap();
        let snapshot = repo.get_snapshot(&id).await.unwrap();
        assert_eq!(Some(PostgresSnapshotStoreAggregateContext::new(id.clone(), 0, 2, TestAggregate {
            id: id.clone(),
            description: "a test description that should be saved".to_string(),
            tests: test_tests.clone(),
        })), snapshot);
    }

}

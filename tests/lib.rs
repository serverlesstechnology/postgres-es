use std::sync::{Arc, RwLock};

use cqrs_es::{Aggregate, AggregateError, DomainEvent, EventEnvelope, EventStore, QueryProcessor};
use serde::{Deserialize, Serialize};

use postgres_es::PostgresStore;

#[derive(Debug, Serialize, Deserialize)]
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

struct TestQuery {
    events: Arc<RwLock<Vec<EventEnvelope<TestAggregate>>>>,
}

impl TestQuery {
    fn new(events: Arc<RwLock<Vec<EventEnvelope<TestAggregate>>>>) -> Self {
        TestQuery { events }
    }
}

impl QueryProcessor<TestAggregate> for TestQuery {
    fn dispatch(&self, _aggregate_id: &str, events: &[EventEnvelope<TestAggregate>]) {
        for event in events {
            let mut event_list = self.events.write().unwrap();
            event_list.push(event.clone());
        }
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

    use postgres_es::{Connection, EventRepository, postgres_cqrs, PostgresSnapshotStore, PostgresStore, PostgresStoreAggregateContext};

    use super::*;

    assert_impl_all!(rdbmsstore; PostgresStore::<TestAggregate>, EventStore::<TestAggregate, PostgresStoreAggregateContext<TestAggregate>>);

    const CONNECTION_STRING: &str = "postgresql://test_user:test_pass@localhost:5432/test";

    fn metadata() -> HashMap<String, String> {
        let now = "2021-03-18T12:32:45.930Z".to_string();
        let mut metadata = HashMap::new();
        metadata.insert("time".to_string(), now);
        metadata
    }

    fn test_store() -> PostgresStore<TestAggregate> {
        PostgresStore::<TestAggregate>::new(Connection::new(CONNECTION_STRING))
    }

    fn test_snapshot_store() -> PostgresSnapshotStore<TestAggregate> {
        PostgresSnapshotStore::<TestAggregate>::new(Connection::new(CONNECTION_STRING))
    }

    #[test]
    fn test_valid_cqrs_framework() {
        let view_events: Arc<RwLock<Vec<EventEnvelope<TestAggregate>>>> = Default::default();
        let query = TestQuery::new(view_events);
        let _ps = postgres_cqrs(Connection::new(CONNECTION_STRING), vec![Box::new(query)]);
    }

    #[tokio::test]
    async fn commit_and_load_events() {
        let event_store = test_store();
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
                metadata(),
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
                metadata(),
            ).await
            .unwrap();
        assert_eq!(3, event_store.load(id.as_str()).await.len());
    }

    #[tokio::test]
    async fn commit_and_load_events_snapshot_store() {
        let event_store = test_snapshot_store();
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
                metadata(),
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
                metadata(),
            ).await
            .unwrap();
        assert_eq!(3, event_store.load(id.as_str()).await.len());
    }

    // #[test]
    // TODO: test no longer valid, is there a way to cover this elsewhere?
    async fn optimistic_lock_error() {
        let event_store = test_store();
        let id = uuid::Uuid::new_v4().to_string();
        assert_eq!(0, event_store.load(id.as_str()).await.len());
        let context = event_store.load_aggregate(id.as_str()).await;

        event_store
            .commit(
                vec![TestEvent::Created(Created {
                    id: "test_event_A".to_string(),
                })],
                context,
                metadata(),
            ).await
            .unwrap();

        let context = event_store.load_aggregate(id.as_str()).await;
        let result = event_store.commit(
            vec![TestEvent::Tested(Tested {
                test_name: "test B".to_string(),
            })],
            context,
            metadata(),
        ).await;
        match result {
            Ok(_) => {
                panic!("expected an optimistic lock error")
            }
            Err(e) => {
                assert_eq!(
                    e,
                    cqrs_es::AggregateError::TechnicalError("optimistic lock error".to_string())
                );
            }
        };
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


    async fn db_pool(connection_string: &str) -> Pool<Postgres> {
        PgPoolOptions::new()
            .max_connections(5)
            .connect(connection_string)
            .await
            .expect("unable to connect to database")
    }

    #[tokio::test]
    async fn event_repositories() {
        let pool = db_pool("postgresql://test_user:test_pass@localhost:5432/test").await;
        let event_repo: EventRepository<TestAggregate> = EventRepository::new(pool.clone());
        event_repo.get_events("52e1d469-df83-44a6-9ac1-d55838502ef1".to_string()).await.unwrap()
            .iter()
            .for_each(|e| println!("{:#?}", e));
        event_repo.insert_events(vec![
            EventEnvelope {
                aggregate_id: "52e1d469-df83-44a6-9ac1-d55838502ef1".to_string(),
                sequence: 5,
                aggregate_type: TestAggregate::aggregate_type().to_string(),
                payload: TestEvent::SomethingElse(SomethingElse { description: "inserting another".to_string() }),
                metadata: Default::default(),
            },
            EventEnvelope {
                aggregate_id: "52e1d469-df83-44a6-9ac1-d55838502ef1".to_string(),
                sequence: 3,
                aggregate_type: TestAggregate::aggregate_type().to_string(),
                payload: TestEvent::SomethingElse(SomethingElse { description: "this should reject".to_string() }),
                metadata: Default::default(),
            }
        ]).await.unwrap();
    }
}

#[test]
fn thread_safe_test() {
    // TODO: use R2D2 for sync/send
    // https://github.com/sfackler/r2d2-postgres
    // fn is_sync<T: Sync>() {}
    // is_sync::<PostgresStore<TestAggregate>>();
    fn is_send<T: Send>() {}
    is_send::<PostgresStore<TestAggregate>>();
}

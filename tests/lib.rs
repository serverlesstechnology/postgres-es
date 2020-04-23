use std::rc::Rc;
use std::sync::RwLock;

use cqrs_es::{Aggregate, AggregateError, Command, DomainEvent, EventStore, MessageEnvelope};
use cqrs_es::view::ViewProcessor;
use postgres::{Connection, TlsMode};
use serde::{Deserialize, Serialize};

use postgres_es::PostgresStore;

#[derive(Debug, Serialize, Deserialize)]
pub struct TestAggregate {
    id: String,
    description: String,
    tests: Vec<String>,
}

impl Aggregate for TestAggregate { fn aggregate_type() -> &'static str { "TestAggregate" } }

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
    pub id: String
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Tested {
    pub test_name: String
}

impl DomainEvent<TestAggregate> for Tested {
    fn apply(self, aggregate: &mut TestAggregate) {
        aggregate.tests.push(self.test_name);
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct SomethingElse {
    pub description: String
}

impl DomainEvent<TestAggregate> for TestEvent {
    fn apply(self, aggregate: &mut TestAggregate) {
        match self {
            TestEvent::Created(e) => {
                aggregate.id = e.id;
            }
            TestEvent::Tested(e) => { e.apply(aggregate) }
            TestEvent::SomethingElse(e) => {
                aggregate.description = e.description;
            }
        }
    }
}

pub struct CreateTest {
    pub id: String,
}

impl Command<TestAggregate, TestEvent> for CreateTest {
    fn handle(self, _aggregate: &TestAggregate) -> Result<Vec<TestEvent>, AggregateError> {
        let event = TestEvent::Created(Created { id: self.id.to_string() });
        Ok(vec![event])
    }
}

pub struct ConfirmTest<'a> {
    pub test_name: &'a str,
}

impl<'a> Command<TestAggregate, TestEvent> for ConfirmTest<'a> {
    fn handle(self, aggregate: &TestAggregate) -> Result<Vec<TestEvent>, AggregateError> {
        for test in &aggregate.tests {
            if test == &self.test_name {
                return Err(AggregateError::new("test already performed"));
            }
        }
        let event = TestEvent::Tested(Tested { test_name: self.test_name.to_string() });
        Ok(vec![event])
    }
}

pub struct DoSomethingElse {
    pub description: String,
}

impl Command<TestAggregate, TestEvent> for DoSomethingElse {
    fn handle(self, _aggregate: &TestAggregate) -> Result<Vec<TestEvent>, AggregateError> {
        let event = TestEvent::SomethingElse(SomethingElse { description: self.description.clone() });
        Ok(vec![event])
    }
}


struct TestView {
    events: Rc<RwLock<Vec<MessageEnvelope<TestAggregate, TestEvent>>>>
}

impl TestView {
    fn new(events: Rc<RwLock<Vec<MessageEnvelope<TestAggregate, TestEvent>>>>) -> Self { TestView { events } }
}


impl ViewProcessor<TestAggregate, TestEvent> for TestView {
    fn dispatch(&self, _aggregate_id: &str, events: Vec<MessageEnvelope<TestAggregate, TestEvent>>) {
        for event in events {
            let mut event_list = self.events.write().unwrap();
            event_list.push(event);
        }
    }
}

pub type TestMessageEnvelope = MessageEnvelope<TestAggregate, TestEvent>;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::Utc;
    use cqrs_es::{CqrsFramework, TimeMetadataSupplier};
    use serde_json::{Map, Value};
    use static_assertions::assert_impl_all;

    use postgres_es::{postgres_cqrs, PostgresCqrs, PostgresStore};

    use super::*;

    assert_impl_all!(rdbmsstore; PostgresStore::<TestAggregate,TestEvent>, EventStore::<TestAggregate,TestEvent>);

    const CONNECTION_STRING: &str = "postgresql://test_user:test_pass@localhost:5432/test";

    fn metadata() -> HashMap<String, String> {
        let now = Utc::now();
        let mut metadata = HashMap::new();
        metadata.insert("time".to_string(), now.to_rfc3339());
        metadata
    }

    fn test_store() -> PostgresStore<TestAggregate, TestEvent> {
        let conn = Connection::connect(CONNECTION_STRING, TlsMode::None).unwrap();
        PostgresStore::<TestAggregate, TestEvent>::new(conn)
    }

    #[test]
    fn test_valid_cqrs_framework() {
        let view_events: Rc<RwLock<Vec<MessageEnvelope<TestAggregate, TestEvent>>>> = Default::default();
        let view = TestView::new(view_events);
        let conn = Connection::connect(CONNECTION_STRING, TlsMode::None).unwrap();
        let ps = postgres_cqrs(conn, Rc::new(view));
    }

    #[test]
    // #[ignore] // integration testing
    fn commit_and_load_events() {
        let event_store = test_store();
        let id = uuid::Uuid::new_v4().to_string();
        assert_eq!(0, event_store.load(id.as_str()).len());

        event_store.commit(vec![
            TestMessageEnvelope::new_with_metadata(
                id.clone(),
                0,
                TestAggregate::aggregate_type().to_string(),
                TestEvent::Created(Created { id: "test_event_A".to_string() }),
                metadata(),
            ),
            TestMessageEnvelope::new_with_metadata(
                id.clone(),
                1,
                TestAggregate::aggregate_type().to_string(),
                TestEvent::Tested(Tested { test_name: "test A".to_string() }),
                metadata()),
        ]);

        assert_eq!(2, event_store.load(id.as_str()).len());

        event_store.commit(vec![
            TestMessageEnvelope::new_with_metadata(
                id.clone(),
                2,
                TestAggregate::aggregate_type().to_string(),
                TestEvent::Tested(Tested { test_name: "test B".to_string() }),
                metadata()),
        ]);
        assert_eq!(3, event_store.load(id.as_str()).len());
    }

    #[test]
    fn optimistic_lock_error() {
        let event_store = test_store();
        let id = uuid::Uuid::new_v4().to_string();
        assert_eq!(0, event_store.load(id.as_str()).len());

        event_store.commit(vec![
            TestMessageEnvelope::new_with_metadata(
                id.clone(),
                0,
                TestAggregate::aggregate_type().to_string(),
                TestEvent::Created(Created { id: "test_event_A".to_string() }),
                metadata(),
            )
        ]);
        match event_store.commit(vec![
            TestMessageEnvelope::new_with_metadata(
                id.clone(),
                0,
                TestAggregate::aggregate_type().to_string(),
                TestEvent::Tested(Tested { test_name: "test B".to_string() }),
                metadata()),
        ]) {
            Ok(_) => { panic!("expected an optimistic lock error") }
            Err(e) => {
                assert_eq!(e, cqrs_es::AggregateError::TechnicalError("optimistic lock error".to_string()));
            }
        };
    }

    #[test]
    fn test_event_breakout_type() {
        let event = TestEvent::Created(Created { id: "test_event_A".to_string() });

        let (event_type, value) = serialize_event(&event);
        println!("{} - {}", &event_type, &value);
        let deser : TestEvent = deserialize_event(event_type.as_str(),value);
        assert_eq!(deser, event);
    }

    fn serialize_event<A, E: DomainEvent<A>>(event: &E) -> (String, Value)
        where A: Aggregate,
              E: DomainEvent<A>
    {
        let val = serde_json::to_value(event).unwrap();
        match &val {
            Value::Object(object) => {
                for key in object.keys() {
                    let value = object.get(key).unwrap();
                    return (key.to_string(), value.clone());
                }
                panic!("{:?} not a domain event", val);
            }
            _ => { panic!("{:?} not an object", val); }
        }
    }

    fn deserialize_event<A, E: DomainEvent<A>>(event_type: &str, value: Value) -> E
        where A: Aggregate,
              E: DomainEvent<A> {
        let mut new_val_map = Map::with_capacity(1);
        new_val_map.insert(event_type.to_string(), value);
        let new_event_val = Value::Object(new_val_map);
        serde_json::from_value(new_event_val).unwrap()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use async_trait::async_trait;
    use std::collections::HashMap;

    use cqrs_es::persist::{
        GenericQuery, PersistedEventStore, PersistedSnapshotStore, SerializedEvent,
        SerializedSnapshot,
    };
    use cqrs_es::{Aggregate, AggregateError, DomainEvent, EventEnvelope, UserErrorPayload, View};
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use sqlx::{Pool, Postgres};

    use crate::query_repository::PostgresViewRepository;
    use crate::PostgresEventRepository;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    pub(crate) struct TestAggregate {
        pub(crate) id: String,
        pub(crate) description: String,
        pub(crate) tests: Vec<String>,
    }

    #[async_trait]
    impl Aggregate for TestAggregate {
        type Command = TestCommand;
        type Event = TestEvent;
        type Error = UserErrorPayload;

        fn aggregate_type() -> &'static str {
            "TestAggregate"
        }

        async fn handle(
            &self,
            _command: Self::Command,
        ) -> Result<Vec<Self::Event>, AggregateError<Self::Error>> {
            Ok(vec![])
        }

        fn apply(&mut self, _e: Self::Event) {}
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
    pub(crate) enum TestEvent {
        Created(Created),
        Tested(Tested),
        SomethingElse(SomethingElse),
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
    pub(crate) struct Created {
        pub id: String,
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
    pub(crate) struct Tested {
        pub test_name: String,
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
    pub struct SomethingElse {
        pub description: String,
    }

    impl DomainEvent for TestEvent {
        fn event_type(&self) -> &'static str {
            match self {
                TestEvent::Created(_) => "Created",
                TestEvent::Tested(_) => "Tested",
                TestEvent::SomethingElse(_) => "SomethingElse",
            }
        }

        fn event_version(&self) -> &'static str {
            "1.0"
        }
    }

    pub(crate) enum TestCommand {}

    pub(crate) type TestQueryRepository =
        GenericQuery<PostgresViewRepository<TestView, TestAggregate>, TestView, TestAggregate>;

    #[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
    pub(crate) struct TestView {
        pub(crate) events: Vec<TestEvent>,
    }

    impl View<TestAggregate> for TestView {
        fn update(&mut self, event: &EventEnvelope<TestAggregate>) {
            self.events.push(event.payload.clone());
        }
    }

    pub(crate) const TEST_CONNECTION_STRING: &str =
        "postgresql://test_user:test_pass@localhost:5432/test";

    pub(crate) async fn new_test_event_store(
        pool: Pool<Postgres>,
    ) -> PersistedEventStore<PostgresEventRepository, TestAggregate> {
        let repo = PostgresEventRepository::new(pool);
        PersistedEventStore::<PostgresEventRepository, TestAggregate>::new(repo)
    }

    pub(crate) async fn new_test_snapshot_store(
        pool: Pool<Postgres>,
    ) -> PersistedSnapshotStore<PostgresEventRepository, TestAggregate> {
        let repo = PostgresEventRepository::new(pool.clone());
        PersistedSnapshotStore::<PostgresEventRepository, TestAggregate>::new(repo)
    }

    pub(crate) fn new_test_metadata() -> HashMap<String, String> {
        let now = "2021-03-18T12:32:45.930Z".to_string();
        let mut metadata = HashMap::new();
        metadata.insert("time".to_string(), now);
        metadata
    }

    pub(crate) fn test_event_envelope(
        id: &str,
        sequence: usize,
        event: TestEvent,
    ) -> SerializedEvent {
        let payload: Value = serde_json::to_value(&event).unwrap();
        SerializedEvent {
            aggregate_id: id.to_string(),
            sequence,
            aggregate_type: TestAggregate::aggregate_type().to_string(),
            event_type: event.event_type().to_string(),
            event_version: event.event_version().to_string(),
            payload,
            metadata: Default::default(),
        }
    }

    pub(crate) fn snapshot_context(
        aggregate_id: String,
        current_sequence: usize,
        current_snapshot: usize,
        aggregate: Value,
    ) -> SerializedSnapshot {
        SerializedSnapshot {
            aggregate_id,
            aggregate,
            current_sequence,
            current_snapshot,
        }
    }
}

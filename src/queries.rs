use std::fmt::Debug;
use std::marker::PhantomData;

use crate::error::PostgresAggregateError;
use async_trait::async_trait;
use cqrs_es::{Aggregate, AggregateError, EventEnvelope, Query, QueryProcessor};
use serde::de::DeserializeOwned;
use serde::Serialize;
use sqlx::postgres::PgRow;
use sqlx::{Pool, Postgres, Row};

/// A simple query repository that can be used both to return deserialized
/// views and to act as a query processor.
pub struct GenericQueryRepository<V, A>
where
    V: Query<A>,
    A: Aggregate,
{
    pool: Pool<Postgres>,
    query_name: String,
    error_handler: Option<Box<ErrorHandler>>,
    _phantom: PhantomData<(V, A)>,
}

type ErrorHandler = dyn Fn(AggregateError) + Send + Sync + 'static;

// mod doc {
//     use crate::{GenericQueryRepository, PostgresStore};
//     use cqrs_es::{Aggregate, AggregateError, CqrsFramework, DomainEvent, EventEnvelope, Query};
//     use serde::de::DeserializeOwned;
//     use serde::{Deserialize, Deserializer, Serialize, Serializer};
//     use sqlx::{Pool, Postgres};
//
//     #[derive(Debug)]
//     enum MyError {
//         NotImplemented,
//     }
//
//     #[derive(Debug)]
//     enum MyCommand {
//         Start,
//         Quit,
//     }
//     #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
//     enum MyEvent {
//         Started,
//         Finished,
//     }
//
//     impl DomainEvent for MyEvent {}
//     #[derive(Debug, Default, Serialize, Deserialize)]
//     struct MyAggregate;
//
//     impl Aggregate for MyAggregate {
//         type Command = MyCommand;
//         type Event = MyEvent;
//
//         fn aggregate_type() -> &'static str {
//             "my-aggregate"
//         }
//
//         fn handle(&self, command: Self::Command) -> Result<Vec<Self::Event>, AggregateError> {
//             match command {
//                 MyCommand::Start => Ok(vec![MyEvent::Started]),
//                 MyCommand::Quit => Ok(vec![MyEvent::Finished]),
//             }
//         }
//
//         fn apply(&mut self, event: Self::Event) {
//             todo!()
//         }
//     }
//
//     #[derive(Debug, Default, Serialize, Deserialize)]
//     struct MyQuery;
//     impl Query<MyAggregate> for MyQuery {
//         fn update(&mut self, event: &EventEnvelope<MyAggregate>) {
//             todo!()
//         }
//     }
//     #[test]
//     fn test() {}
//     async fn configure_me(pool: Pool<Postgres>) {
//         let mut query_repository =
//             GenericQueryRepository::<MyQuery, MyAggregate>::new("my-query", pool.clone());
//         query_repository.with_error_handler(Box::new(|e| panic!("{}", e)));
//         let query = query_repository.load("customer-B24DA0".to_string()).await;
//
//         let store = PostgresStore::<MyAggregate>::new(pool);
//         let cqrs = CqrsFramework::new(store, vec![Box::new(query_repository)]);
//     }
// }
impl<V, A> GenericQueryRepository<V, A>
where
    V: Query<A>,
    A: Aggregate,
{
    /// Creates a new `GenericQueryRepository` that will store its' views in the table named
    /// identically to the `query_name` value provided. This table should be created by the user
    /// before using this query repository (see `/db/init.sql` sql initialization file).
    ///
    /// ```ignore
    /// let query_repository = GenericQueryRepository::<MyQuery,MyAggregate>::new("my-query", pool);
    /// let store = PostgresStore::<MyAggregate>::new(pool);
    /// let cqrs = CqrsFramework::new(store, vec![Box::new(query_repository)]);
    /// ```
    #[must_use]
    pub fn new(query_name: &str, pool: Pool<Postgres>) -> Self {
        GenericQueryRepository {
            pool,
            query_name: query_name.to_string(),
            error_handler: None,
            _phantom: PhantomData,
        }
    }
    /// Allows the user to apply a custom error handler to the query.
    /// Queries are infallible and _should_ never cause errors,
    /// but programming errors or other technical problems
    /// could, and this is where the user should log or otherwise register the issue.
    ///
    /// This is not required for usage but without an error handler any error encountered
    /// by the query repository will simply be ignored.
    ///
    /// _An error handler that panics on any error._
    /// ```ignore
    /// query_repository.with_error_handler(Box::new(|e|panic!("{}",e)));
    /// ```
    pub fn with_error_handler(&mut self, error_handler: Box<ErrorHandler>) {
        self.error_handler = Some(error_handler);
    }

    async fn load_mut(
        &self,
        query_instance_id: String,
    ) -> Result<(V, QueryContext<V>), PostgresAggregateError> {
        let query = format!(
            "SELECT version,payload FROM {} WHERE query_instance_id= $1",
            &self.query_name
        );
        let row: Option<PgRow> = sqlx::query(&query)
            .bind(&query_instance_id)
            .fetch_optional(&self.pool)
            .await?;
        match row {
            None => {
                let view_context = QueryContext {
                    query_name: self.query_name.clone(),
                    query_instance_id,
                    version: 0,
                    _phantom: PhantomData,
                };
                Ok((Default::default(), view_context))
            }
            Some(row) => {
                let view_name = self.query_name.clone();
                let version = row.get("version");
                let payload = row.get("payload");
                let view = serde_json::from_str(payload)?;
                let view_context = QueryContext {
                    query_name: view_name,
                    query_instance_id,
                    version,
                    _phantom: PhantomData,
                };
                Ok((view, view_context))
            }
        }
    }

    pub(crate) async fn apply_events(
        &self,
        query_instance_id: &str,
        events: &[EventEnvelope<A>],
    ) -> Result<(), PostgresAggregateError> {
        let (mut view, view_context) = self.load_mut(query_instance_id.to_string()).await?;
        for event in events {
            view.update(event);
        }
        view_context.commit(self.pool.clone(), view).await?;
        Ok(())
    }

    fn handle_error(&self, error: AggregateError) {
        match &self.error_handler {
            None => {}
            Some(handler) => {
                (handler)(error);
            }
        }
    }
    fn handle_internal_error(&self, error: PostgresAggregateError) {
        self.handle_error(error.into());
    }

    /// Loads and deserializes a view based on the view id.
    /// Use this method to load a query when requested by a user.
    ///
    /// This is an asynchronous method so don't forget to `await`.
    ///
    /// ```ignore
    /// let query = query_repository.load("customer-B24DA0".to_string()).await;
    /// ```
    pub async fn load(&self, query_instance_id: String) -> Option<V> {
        let query = format!(
            "SELECT version,payload FROM {} WHERE query_instance_id= $1",
            &self.query_name
        );
        let row_option: Option<PgRow> = match sqlx::query(&query)
            .bind(&query_instance_id)
            .fetch_optional(&self.pool)
            .await
        {
            Ok(result) => result,
            Err(e) => {
                self.handle_internal_error(e.into());
                return None;
            }
        };
        match row_option {
            Some(row) => self.deser_view(row),
            None => None,
        }
    }

    fn deser_view(&self, row: PgRow) -> Option<V> {
        let payload = row.get("payload");
        match serde_json::from_str(payload) {
            Ok(view) => Some(view),
            Err(e) => {
                self.handle_internal_error(e.into());
                None
            }
        }
    }
}

#[async_trait]
impl<Q, A> QueryProcessor<A> for GenericQueryRepository<Q, A>
where
    Q: Query<A> + Send + Sync + 'static,
    A: Aggregate,
{
    async fn dispatch(&self, query_instance_id: &str, events: &[EventEnvelope<A>]) {
        match self
            .apply_events(&query_instance_id.to_string(), events)
            .await
        {
            Ok(_) => {}
            Err(err) => self.handle_internal_error(err),
        };
    }
}

struct QueryContext<V>
where
    V: Debug + Default + Serialize + DeserializeOwned + Default,
{
    query_name: String,
    query_instance_id: String,
    version: i64,
    _phantom: PhantomData<V>,
}

impl<V> QueryContext<V>
where
    V: Debug + Default + Serialize + DeserializeOwned + Default,
{
    async fn commit(&self, pool: Pool<Postgres>, view: V) -> Result<(), PostgresAggregateError> {
        let sql = match self.version {
            0 => format!(
                "INSERT INTO {} (payload, version, query_instance_id) VALUES ( $1, $2, $3 )",
                &self.query_name
            ),
            _ => format!(
                "UPDATE {} SET payload= $1 , version= $2 WHERE query_instance_id= $3",
                &self.query_name
            ),
        };
        let version = self.version + 1;
        let payload = serde_json::to_string(&view).expect("failed to serialize view");
        sqlx::query(sql.as_str())
            .bind(payload)
            .bind(&version)
            .bind(&self.query_instance_id)
            .execute(&pool)
            .await?;
        Ok(())
    }
}

use std::rc::Rc;

use cqrs_es::{Aggregate, CqrsFramework, DomainEvent, TimeMetadataSupplier};
use cqrs_es::view::ViewProcessor;
use postgres::Connection;

use crate::PostgresStore;

/// A convenience type for creating a CqrsFramework backed by PostgresStore and using a simple
/// metadata supplier with time of commit.
pub type PostgresCqrs<A, E> = CqrsFramework<A, E, PostgresStore<A, E>, TimeMetadataSupplier>;

/// A convenience function for creating a CqrsFramework
pub fn postgres_cqrs<A, E>(conn: Connection, view: Rc<dyn ViewProcessor<A, E>>) -> PostgresCqrs<A, E>
    where A: Aggregate,
          E: DomainEvent<A>
{
    let store = PostgresStore::new(conn);
    let metadata_supplier = TimeMetadataSupplier::default();
    CqrsFramework::new(store, view, metadata_supplier)
}


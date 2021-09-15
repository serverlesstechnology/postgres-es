
use r2d2_postgres::{postgres::NoTls, PostgresConnectionManager};
use r2d2_postgres::r2d2::{Pool, PooledConnection};

pub struct Connection {
    pool: Pool<PostgresConnectionManager<NoTls>>
}

impl Connection {
    pub fn new(connection_string: &str) -> Self {
        let manager = PostgresConnectionManager::new(
            connection_string.parse().unwrap(),
            NoTls,
        );
        let pool = Pool::new(manager).unwrap();
        Self {
            pool
        }
    }
    pub fn conn(&self) -> PooledConnection<PostgresConnectionManager<NoTls>> {
        self.pool.get().unwrap()
    }
}
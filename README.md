# postgres-es

> A Postgres implementation of the `EventStore` trait in cqrs-es.

[![Crates.io](https://img.shields.io/crates/v/postgres-es)](https://crates.io/crates/postgres-es)
[![docs](https://img.shields.io/badge/API-docs-blue.svg)](https://docs.rs/postgres-es)
---

## Usage

Add to your Cargo.toml file:

```toml
[dependencies]
cqrs-es = "0.2.3"
persist-es = "0.2.3"
postgres-es = "0.2.3"
```

Requires access to a Postgres DB with existing tables. See:
- [Sample database configuration](db/init.sql)
- Use `docker-compose` to quickly setup [a local database](docker-compose.yml)

A simple configuration example:
```
let store = default_postgress_pool("postgresql://my_user:my_pass@localhost:5432/my_db");
let cqrs = postgres_es::postgres_cqrs(pool, vec![])
```

## Change log

#### `v0.2.3`
- Added upcasters to event stores.

#### `v0.2.2`
- Consolidated repositories to a single trait encompassing all functionality.

#### `v0.2.1`
- Moved generic persistence logic into cqrs-es package.
- Added repositories.
- Added event context information to event envelope.

#### `v0.2.0`
Moved to async/await for better tool support.

#### `v0.1.3`
Aggregates now consume events on `apply`.

#### `v0.1.2`
Require queries to also use Send & Sync.

#### `v0.1.1`
Use r2d2_postgres crate to support `Send + Sync` for multi-threaded applications.

## TODOs
- Add support for TLS.
- Additional framework around `GenericQueryRepository` to simplify event replay.

## Demo
A demo application [is available here](https://github.com/serverlesstechnology/cqrs-demo).
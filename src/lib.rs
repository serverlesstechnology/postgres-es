#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(clippy::all)]
// #![warn(clippy::pedantic)]
//! # postgres-es
//!
//! > A Postgres implementation of the `EventStore` trait in [cqrs-es](https://crates.io/crates/persist-es).
//!
pub use crate::cqrs::*;
pub use crate::event_repository::*;
pub use crate::event_repository::*;
pub use crate::query_repository::*;
pub use crate::types::*;

mod cqrs;
mod error;
mod event_repository;
mod query_repository;
mod testing;
mod types;

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(clippy::all)]
// #![warn(clippy::pedantic)]
//! # postgres-es
//!
//! > A Postgres implementation of the `EventStore` trait in cqrs-es.
//!
//! ![Build tag](https://codebuild.us-west-2.amazonaws.com/badges?uuid=eyJlbmNyeXB0ZWREYXRhIjoiVVUyR0tRbTZmejFBYURoTHdpR3FnSUFqKzFVZE9JNW5haDZhcUFlY2xtREhtaVVJMWsxcWZOeC8zSUR0UWhpaWZMa0ZQSHlEYjg0N2FoU2lwV1FsTXFRPSIsIml2UGFyYW1ldGVyU3BlYyI6IldjUVMzVEpKN1V3aWxXWGUiLCJtYXRlcmlhbFNldFNlcmlhbCI6MX0%3D&branch=master)
//! [![Crates.io](https://img.shields.io/crates/v/postgres-es)](https://crates.io/crates/postgres-es)
//! [![docs](https://img.shields.io/badge/API-docs-blue.svg)](https://docs.rs/postgres-es)
//!
//! ## Installation
//!
//!
//! ```toml
//! [dependencies]
//! postgres-es = "0.0.4"
//! ```
pub use crate::store::*;
pub use crate::queries::*;
pub use crate::cqrs::*;

mod store;
mod queries;
mod cqrs;


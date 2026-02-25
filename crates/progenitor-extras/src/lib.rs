//! Extra functionality for the [Progenitor](https://docs.rs/progenitor) OpenAPI client generator.
//!
//! ## Operation retries
//!
//! The [`retry`] module provides utilities to perform retries against
//! Progenitor-generated API clients with a backoff via the [`backon`] crate. See
//! the module documentation for more information.

#![deny(missing_docs)]

pub mod retry;

pub use backon;

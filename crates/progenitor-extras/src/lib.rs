//! Extra functionality for the [Progenitor](https://docs.rs/progenitor) OpenAPI client generator.
//!
//! ## Operation retries
//!
//! With the `retry` feature enabled, the [`retry`] module provides utilities to
//! perform retries against Progenitor-generated API clients with a backoff via
//! the [`backon`] crate. See the module documentation for more information.
//!
//! ## Logging with slog
//!
//! With the `slog` feature enabled, the [`slog_hooks`] module provides
//! `ClientHooks` implementations that log requests and responses via
//! [slog](https://docs.rs/slog). See the module documentation for more
//! information.
//!
//! ## Features
//!
//! - `retry`: Enables the `retry` module. *Disabled by default.*
//! - `slog`: Enables the `slog_hooks` module. *Disabled by default.*

#![deny(missing_docs)]
#![cfg_attr(doc_cfg, feature(doc_cfg))]

#[cfg(feature = "retry")]
pub mod retry;
#[cfg(feature = "slog")]
pub mod slog_hooks;

#[cfg(feature = "retry")]
pub use backon;

#[cfg(feature = "slog")]
#[doc(hidden)]
pub mod __private {
    pub use progenitor_client;
    pub use reqwest;
    pub use slog;
}

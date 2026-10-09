<!-- cargo-sync-rdme title [[ -->
# progenitor-extras
<!-- cargo-sync-rdme ]] -->
<!-- cargo-sync-rdme badge [[ -->
![License: MIT OR Apache-2.0](https://img.shields.io/crates/l/progenitor-extras.svg?)
[![crates.io](https://img.shields.io/crates/v/progenitor-extras.svg?logo=rust)](https://crates.io/crates/progenitor-extras)
[![docs.rs](https://img.shields.io/docsrs/progenitor-extras.svg?logo=docs.rs)](https://docs.rs/progenitor-extras)
[![Rust: ^1.88.0](https://img.shields.io/badge/rust-^1.88.0-93450a.svg?logo=rust)](https://doc.rust-lang.org/cargo/reference/manifest.html#the-rust-version-field)
<!-- cargo-sync-rdme ]] -->
<!-- cargo-sync-rdme rustdoc [[ -->
Extra functionality for the [Progenitor](https://docs.rs/progenitor) OpenAPI client generator.

### Operation retries

With the `retry` feature enabled, the [`retry`] module provides utilities to
perform retries against Progenitor-generated API clients with a backoff via
the [`backon`] crate. See the module documentation for more information.

### Logging with slog

With the `slog` feature enabled, the [`slog_hooks`] module provides
`ClientHooks` implementations that log requests and responses via
[slog](https://docs.rs/slog). See the module documentation for more
information.

### Features

* `retry`: Enables the `retry` module. *Disabled by default.*
* `slog`: Enables the `slog_hooks` module. *Disabled by default.*

[`retry`]: https://docs.rs/progenitor-extras/0.3.0/progenitor_extras/retry/index.html "mod progenitor_extras::retry"
[`backon`]: https://docs.rs/backon/1.6.0/backon/index.html "mod backon"
[`slog_hooks`]: https://docs.rs/progenitor-extras/0.3.0/progenitor_extras/slog_hooks/index.html "mod progenitor_extras::slog_hooks"
<!-- cargo-sync-rdme ]] -->

## License

This project is available under the terms of either the [Apache 2.0 license](LICENSE-APACHE) or the [MIT license](LICENSE-MIT).

# Changelog

<!-- next-header -->
## Unreleased - ReleaseDate

### Added

- A new `slog` feature, which enables the `slog_hooks` module. This module provides an `impl_slog_client_hooks!` macro which implements Progenitor's `ClientHooks` for clients generated with `inner_type = slog::Logger`, logging each request and response at debug level.

  The underlying `log_request!` and `log_response!` macros are also available for use in hand-written `ClientHooks` implementations.

### Changed

- Updated `progenitor-client` dependency from 0.14.0 to 0.15.1.

- The `retry` module and the `backon` re-export are now behind a new `retry` feature, which is disabled by default. With this change, users of `slog_hooks` no longer depend on `backon`.

  To migrate, add `features = ["retry"]` to the `progenitor-extras` dependency.

- The retry error types now store the underlying `progenitor_client::Error<E>` in a `Box`.

  To migrate, use `*e` to get the owned `progenitor_client::Error<E>` back. Method calls such as `e.status()` keep working through auto-deref. `RetryNotification::error` is unchanged.

## [0.2.0] - 2026-04-26

### Changed

- Updated `progenitor-client` dependency from 0.13.0 to 0.14.0.

## [0.1.1] - 2026-03-09

### Added

- `retry_operation_indefinitely` and `retry_operation_while_indefinitely` to retry operations without a limit on the number of retries. These are provided as separate functions for two reasons:
  - The error types are simpler since they don't have to model that retries were exhausted.
  - Workspaces that have indefinite retries as a correctness requirement can use clippy's [`disallowed_methods` lint](https://rust-lang.github.io/rust-clippy/master/index.html#disallowed_methods) to ban the use of the non-indefinite versions.

## [0.1.0] - 2026-02-25

### Added

- Initial release with the `retry` module, providing:
  - `retry_operation` for retrying Progenitor client operations with backoff.
  - `retry_operation_while` for retries with a "gone check" that aborts when
    the target is permanently unavailable.
  - `default_retry_policy` for a reasonable default exponential backoff policy.

<!-- next-url -->
[0.2.0]: https://github.com/oxidecomputer/progenitor-extras/releases/tag/progenitor-extras-0.2.0
[0.1.1]: https://github.com/oxidecomputer/progenitor-extras/releases/tag/progenitor-extras-0.1.1
[0.1.0]: https://github.com/oxidecomputer/progenitor-extras/releases/tag/progenitor-extras-0.1.0

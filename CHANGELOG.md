# Changelog

<!-- next-header -->
## Unreleased - ReleaseDate

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
[0.1.0]: https://github.com/oxidecomputer/progenitor-extras/releases/tag/progenitor-extras-0.1.0

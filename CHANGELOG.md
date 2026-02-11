# Changelog

<!-- next-header -->
## Unreleased - ReleaseDate

### Added

- Initial release with the `retry` module, providing:
  - `retry_operation` for retrying Progenitor client operations with backoff.
  - `retry_operation_while` for retries with a "gone check" that aborts when
    the target is permanently unavailable.
  - `default_retry_policy` for a reasonable default exponential backoff policy.

<!-- next-url -->

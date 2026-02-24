//! Retry logic for operations against Progenitor-generated API clients.
//!
//! The primary entry points are:
//!
//! * [`retry_operation`], for retries; and
//! * [`retry_operation_while`], for retries with an additional "gone check"
//!   that can abort the loop when the target is permanently unavailable.
//!
//! Retry uses a backoff policy via the [`backon`] crate. Call
//! [`default_retry_policy`] for a reasonable default, or construct your own
//! [`backon::BackoffBuilder`] for custom behavior.
//!
//! Note that the retry operations currently assume a Tokio backend, matching
//! Progenitor.

use backon::{BackoffBuilder, ExponentialBuilder};
use std::{
    convert::Infallible, error::Error, fmt, future::Future, time::Duration,
};

/// Result of a gone check passed to [`retry_operation_while`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GoneCheckResult {
    /// The target is still available; continue retrying.
    StillAvailable,

    /// The target is permanently gone; abort the retry loop.
    Gone,
}

/// Error produced by [`retry_operation`].
#[derive(Debug)]
pub struct RetryOperationError<E> {
    /// One-indexed attempt number at which the error occurred.
    pub attempt: usize,
    /// The kind of error.
    pub kind: RetryOperationErrorKind<E>,
}

/// The kind of error in a [`RetryOperationError`].
#[derive(Debug)]
pub enum RetryOperationErrorKind<E> {
    /// The operation failed with a non-retryable error.
    OperationError(progenitor_client::Error<E>),

    /// All retry attempts were exhausted without success.
    ///
    /// The contained error is the last retryable error encountered.
    RetriesExhausted(progenitor_client::Error<E>),
}

impl<E> fmt::Display for RetryOperationErrorKind<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OperationError(_) => {
                f.write_str("progenitor API operation failed")
            }
            Self::RetriesExhausted(_) => f.write_str("retries exhausted"),
        }
    }
}

impl<E> fmt::Display for RetryOperationError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "failed at attempt {}: ", self.attempt)?;
        self.kind.fmt(f)
    }
}

impl<E> Error for RetryOperationError<E>
where
    E: fmt::Debug + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            RetryOperationErrorKind::OperationError(e)
            | RetryOperationErrorKind::RetriesExhausted(e) => Some(e),
        }
    }
}

impl<E> RetryOperationError<E> {
    /// Returns true if the underlying operation error is a 404 Not Found.
    pub fn is_not_found(&self) -> bool {
        match &self.kind {
            // In practice, 404 is not retryable, so this will only match
            // OperationError. But something outside of this crate can
            // artificially construct a RetriesExhausted with a 404, so match
            // against that as well.
            RetryOperationErrorKind::OperationError(e)
            | RetryOperationErrorKind::RetriesExhausted(e) => {
                e.status() == Some(http::StatusCode::NOT_FOUND)
            }
        }
    }
}

/// Error produced by [`retry_operation_while`].
#[derive(Debug)]
pub struct RetryOperationWhileError<E, GoneErr = Infallible> {
    /// One-indexed attempt number at which the error occurred.
    pub attempt: usize,
    /// The kind of error.
    pub kind: RetryOperationWhileErrorKind<E, GoneErr>,
}

/// The kind of error in a [`RetryOperationWhileError`].
#[derive(Debug)]
pub enum RetryOperationWhileErrorKind<E, GoneErr = Infallible> {
    /// The gone check indicated that the remote server is permanently
    /// unavailable.
    Gone,

    /// The gone check itself failed.
    GoneCheckError(GoneErr),

    /// The operation failed with a non-retryable error.
    OperationError(progenitor_client::Error<E>),

    /// All retry attempts were exhausted without success.
    ///
    /// The contained error is the last retryable error encountered.
    RetriesExhausted(progenitor_client::Error<E>),
}

impl<E, GoneErr> fmt::Display for RetryOperationWhileErrorKind<E, GoneErr> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Gone => f.write_str("remote server is gone"),
            Self::GoneCheckError(_) => {
                f.write_str("failed to determine whether remote server is gone")
            }
            Self::OperationError(_) => {
                f.write_str("progenitor API operation failed")
            }
            Self::RetriesExhausted(_) => f.write_str("retries exhausted"),
        }
    }
}

impl<E, GoneErr> fmt::Display for RetryOperationWhileError<E, GoneErr> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "failed at attempt {}: ", self.attempt)?;
        self.kind.fmt(f)
    }
}

impl<E, GoneErr> Error for RetryOperationWhileError<E, GoneErr>
where
    E: fmt::Debug + 'static,
    GoneErr: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            RetryOperationWhileErrorKind::Gone => None,
            RetryOperationWhileErrorKind::GoneCheckError(e) => Some(e),
            RetryOperationWhileErrorKind::OperationError(e)
            | RetryOperationWhileErrorKind::RetriesExhausted(e) => Some(e),
        }
    }
}

impl<E, GoneErr> RetryOperationWhileError<E, GoneErr> {
    /// Returns true if the underlying operation error is a 404 Not Found.
    pub fn is_not_found(&self) -> bool {
        match &self.kind {
            // In practice, 404 is not retryable, so this will only match
            // OperationError. But something outside of this crate can
            // artificially construct a RetriesExhausted with a 404, so match
            // against that as well.
            RetryOperationWhileErrorKind::OperationError(e)
            | RetryOperationWhileErrorKind::RetriesExhausted(e) => {
                e.status() == Some(http::StatusCode::NOT_FOUND)
            }
            RetryOperationWhileErrorKind::Gone
            | RetryOperationWhileErrorKind::GoneCheckError(_) => false,
        }
    }

    /// Returns `true` if the remote server is gone.
    pub fn is_gone(&self) -> bool {
        matches!(self.kind, RetryOperationWhileErrorKind::Gone)
    }
}

/// Returns a reasonable default retry policy.
///
/// This policy is an exponential backoff that sets:
///
/// * the initial retry interval to ~250ms (mean, with jitter)
/// * the maximum interval to 3 minutes
/// * a backoff multiplier of 2.0
/// * up to 13 retries
/// * with jitter enabled
///
/// The base delay is set to 167ms rather than 250ms to compensate for
/// `backon`'s additive jitter, which distributes each delay `d` over
/// `[d, 2d)` (mean = 1.5d). With a 167ms base, the mean first retry
/// delay is ~250ms.
pub fn default_retry_policy() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_factor(2.0)
        .with_min_delay(Duration::from_millis(167))
        .with_max_delay(Duration::from_secs(60 * 3))
        .with_max_times(13)
        .with_jitter()
}

/// Data passed into notify functions.
#[derive(Debug)]
#[non_exhaustive]
pub struct RetryNotification<E> {
    /// One-indexed attempt number. The first transient failure produces
    /// `attempt = 1`, the second produces `attempt = 2`, and so on.
    ///
    /// The notify function is not called when retries are exhausted. Instead,
    /// an error is returned.
    pub attempt: usize,
    /// The retryable error that caused this retry. This error is always
    /// retryable (i.e., `error.is_retryable()` returns `true`).
    pub error: progenitor_client::Error<E>,
    /// The delay before the next attempt.
    pub delay: Duration,
}

/// Retries a progenitor client operation until it succeeds or fails
/// permanently.
///
/// Transient (retryable) errors are retried according to the supplied backoff
/// policy. All other errors are returned immediately as
/// [`RetryOperationErrorKind::OperationError`].
///
/// If all retries are exhausted, the last transient error is returned as
/// [`RetryOperationErrorKind::RetriesExhausted`].
///
/// `notify` is called on each transient failure with the error and the delay
/// before the next attempt. It is not called when retries are exhausted;
/// the terminal failure is communicated through the
/// [`RetriesExhausted`](RetryOperationErrorKind::RetriesExhausted) return variant.
///
/// The `operation` must be idempotent.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// use progenitor_extras::retry::{default_retry_policy, retry_operation};
///
/// // In practice, replace the closure body with a progenitor client
/// // call, e.g. `|| async { client.some_endpoint().send().await }`.
/// let result = retry_operation(
///     default_retry_policy(),
///     || async { Ok::<_, progenitor_client::Error<()>>(42u32) },
///     |notification| {
///         eprintln!(
///             "transient error ({:?}), retrying in {:?}",
///             notification.error, notification.delay,
///         );
///     },
/// )
/// .await;
///
/// assert_eq!(result.unwrap(), 42);
/// # }
/// ```
pub async fn retry_operation<T, E, B, N, F, Fut>(
    backoff: B,
    mut operation: F,
    mut notify: N,
) -> Result<T, RetryOperationError<E>>
where
    B: BackoffBuilder,
    N: FnMut(RetryNotification<E>),
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, progenitor_client::Error<E>>>,
{
    // This function implements its own retry loop rather than delegating to
    // backon's `Retryable` trait so that the notify function can be called with
    // an owned error.
    let mut delays = backoff.build();
    let mut attempt = 1;

    loop {
        match (operation)().await {
            Ok(v) => return Ok(v),
            Err(error) => {
                if !error.is_retryable() {
                    return Err(RetryOperationError {
                        attempt,
                        kind: RetryOperationErrorKind::OperationError(error),
                    });
                }
                match delays.next() {
                    Some(delay) => {
                        notify(RetryNotification { attempt, error, delay });
                        tokio::time::sleep(delay).await;
                        attempt += 1;
                    }
                    None => {
                        return Err(RetryOperationError {
                            attempt,
                            kind: RetryOperationErrorKind::RetriesExhausted(
                                error,
                            ),
                        });
                    }
                }
            }
        }
    }
}

/// Retries a progenitor client operation with an additional "gone check."
///
/// This function is intended for service mesh-type scenarios, where a
/// service being gone is determined independently of the operation itself.
///
/// Before each attempt, `gone_check` is called. If it returns
/// `Ok(GoneCheckResult::Gone)`, the loop is aborted with
/// [`RetryOperationWhileErrorKind::Gone`]. If the gone check itself fails, the
/// loop is aborted with [`RetryOperationWhileErrorKind::GoneCheckError`].
///
/// Transient errors (as classified by
/// [`progenitor_client::Error::is_retryable`]) are retried according to the
/// supplied `backoff` policy. Non-retryable errors are returned as
/// [`RetryOperationWhileErrorKind::OperationError`]. If all retries are
/// exhausted, the last transient error is returned as
/// [`RetryOperationWhileErrorKind::RetriesExhausted`].
///
/// Gone-check errors ([`RetryOperationWhileErrorKind::GoneCheckError`]) are
/// treated as permanent and abort the loop immediately. If the gone check
/// itself can fail transiently, handle retries within the `gone_check`
/// closure.
///
/// `notify` is called on each transient failure with the error and the
/// delay before the next attempt. It is not called when retries are
/// exhausted; the terminal failure is communicated through the
/// [`RetryOperationWhileErrorKind::RetriesExhausted`] return variant.
///
/// The `operation` must be idempotent.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// use progenitor_extras::retry::{
///     GoneCheckResult, default_retry_policy, retry_operation_while,
/// };
///
/// // In practice, replace these closure bodies with real client calls
/// // and a real gone check (e.g. querying whether a sled is in service).
/// let result = retry_operation_while(
///     default_retry_policy(),
///     || async { Ok::<_, progenitor_client::Error<()>>(42u32) },
///     || async {
///         Ok::<_, std::convert::Infallible>(GoneCheckResult::StillAvailable)
///     },
///     |notification| {
///         eprintln!(
///             "transient error ({:?}), retrying in {:?}",
///             notification.error, notification.delay,
///         );
///     },
/// )
/// .await;
///
/// assert_eq!(result.unwrap(), 42);
/// # }
/// ```
///
/// The gone check can abort the loop early:
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// use progenitor_extras::retry::{
///     GoneCheckResult, RetryOperationWhileError, default_retry_policy,
///     retry_operation_while,
/// };
///
/// let result: Result<(), RetryOperationWhileError<()>> =
///     retry_operation_while(
///         default_retry_policy(),
///         || async { Ok::<_, progenitor_client::Error<()>>(()) },
///         // Target is gone; abort immediately.
///         || async {
///             Ok::<_, std::convert::Infallible>(GoneCheckResult::Gone)
///         },
///         |_notification| {},
///     )
///     .await;
///
/// assert!(result.unwrap_err().is_gone());
/// # }
/// ```
pub async fn retry_operation_while<T, E, GoneErr, B, N, F, Fut, GF, GFut>(
    backoff: B,
    mut operation: F,
    mut gone_check: GF,
    mut notify: N,
) -> Result<T, RetryOperationWhileError<E, GoneErr>>
where
    B: BackoffBuilder,
    N: FnMut(RetryNotification<E>),
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, progenitor_client::Error<E>>>,
    GF: FnMut() -> GFut,
    GFut: Future<Output = Result<GoneCheckResult, GoneErr>>,
{
    // This function implements its own retry loop rather than delegating
    // to backon's `Retryable` trait so that:
    //
    // * the gone check can be interleaved before each operation attempt.
    // * the notify function can be called with an owned error.
    let mut delays = backoff.build();

    let mut attempt = 1;
    loop {
        // Check if the target is still available before attempting
        // the operation.
        //
        // An interesting question is: in this loop, should `gone_check` be
        // called before or after `operation`? There is an inherent TOCTTOU race
        // between `gone_check` and `operation`, and both before and after are
        // defensible. We call `gone_check` before `operation` to maintain
        // parity with Omicron from which this code was adapted, but we may want
        // to change this in the future.
        match (gone_check)().await {
            Ok(GoneCheckResult::Gone) => {
                return Err(RetryOperationWhileError {
                    attempt,
                    kind: RetryOperationWhileErrorKind::Gone,
                });
            }
            Ok(GoneCheckResult::StillAvailable) => {}
            Err(e) => {
                return Err(RetryOperationWhileError {
                    attempt,
                    kind: RetryOperationWhileErrorKind::GoneCheckError(e),
                });
            }
        }

        match (operation)().await {
            Ok(v) => return Ok(v),
            Err(error) => {
                if !error.is_retryable() {
                    return Err(RetryOperationWhileError {
                        attempt,
                        kind: RetryOperationWhileErrorKind::OperationError(
                            error,
                        ),
                    });
                }
                match delays.next() {
                    Some(delay) => {
                        notify(RetryNotification { attempt, error, delay });
                        tokio::time::sleep(delay).await;
                        attempt += 1;
                    }
                    None => {
                        return Err(RetryOperationWhileError {
                            attempt,
                            kind:
                                RetryOperationWhileErrorKind::RetriesExhausted(
                                    error,
                                ),
                        });
                    }
                }
            }
        }
    }
}

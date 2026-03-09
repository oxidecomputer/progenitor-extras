use backon::BackoffBuilder;
use http::{StatusCode, header::HeaderMap};
use progenitor_client::{Error, ResponseValue};
use progenitor_extras::retry::{
    GoneCheckResult, IndefiniteBackoffParams, IndefiniteRetryOperationError,
    IndefiniteRetryOperationWhileError, IndefiniteRetryOperationWhileErrorKind,
    RetryOperationError, RetryOperationErrorKind, RetryOperationWhileError,
    RetryOperationWhileErrorKind, default_indefinite_retry_policy,
    retry_operation, retry_operation_indefinitely, retry_operation_while,
    retry_operation_while_indefinitely,
};
use std::{convert::Infallible, future::Future, time::Duration};

// ---
// Helpers
// ---

/// Returns a backoff policy suitable for tests: zero delay, limited retries.
fn test_backoff(max_retries: usize) -> backon::ConstantBuilder {
    backon::ConstantBuilder::default()
        .with_delay(Duration::ZERO)
        .with_max_times(max_retries)
}

/// Constructs a retryable error (503 Service Unavailable).
fn retryable_error() -> Error<()> {
    Error::ErrorResponse(ResponseValue::new(
        (),
        StatusCode::SERVICE_UNAVAILABLE,
        HeaderMap::new(),
    ))
}

/// Constructs a non-retryable error.
fn permanent_error() -> Error<()> {
    Error::InvalidRequest("permanent error".to_string())
}

/// Constructs a 404 Not Found error.
fn not_found_error() -> Error<()> {
    Error::ErrorResponse(ResponseValue::new(
        (),
        StatusCode::NOT_FOUND,
        HeaderMap::new(),
    ))
}

/// Output of [`test_retry_operation`].
struct TestRetryOutput<T, E> {
    result: Result<T, RetryOperationError<E>>,
    call_count: usize,
    notify_count: usize,
}

/// Wraps [`retry_operation`] with automatic counter tracking and
/// notification-attempt assertions.
async fn test_retry_operation<T, E, B, F, Fut>(
    backoff: B,
    mut operation: F,
) -> TestRetryOutput<T, E>
where
    B: BackoffBuilder,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Error<E>>>,
{
    let mut call_count = 0usize;
    let mut notify_count = 0usize;

    let result = retry_operation(
        backoff,
        || {
            call_count += 1;
            operation()
        },
        |notification| {
            let expected = notify_count + 1;
            assert_eq!(
                notification.attempt, expected,
                "expected attempt {expected}, got {}",
                notification.attempt,
            );
            notify_count += 1;
        },
    )
    .await;

    TestRetryOutput { result, call_count, notify_count }
}

/// Output of [`test_retry_operation_while`].
struct TestRetryWhileOutput<T, E, GoneErr = Infallible> {
    result: Result<T, RetryOperationWhileError<E, GoneErr>>,
    call_count: usize,
    gone_check_count: usize,
    notify_count: usize,
}

/// Wraps [`retry_operation_while`] with automatic counter tracking and
/// notification-attempt assertions.
async fn test_retry_operation_while<T, E, GoneErr, B, F, Fut, GF, GFut>(
    backoff: B,
    mut operation: F,
    mut gone_check: GF,
) -> TestRetryWhileOutput<T, E, GoneErr>
where
    B: BackoffBuilder,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Error<E>>>,
    GF: FnMut() -> GFut,
    GFut: Future<Output = Result<GoneCheckResult, GoneErr>>,
{
    let mut call_count = 0usize;
    let mut gone_check_count = 0usize;
    let mut notify_count = 0usize;

    let result = retry_operation_while(
        backoff,
        || {
            call_count += 1;
            operation()
        },
        || {
            gone_check_count += 1;
            gone_check()
        },
        |notification| {
            let expected = notify_count + 1;
            assert_eq!(
                notification.attempt, expected,
                "expected attempt {expected}, got {}",
                notification.attempt,
            );
            notify_count += 1;
        },
    )
    .await;

    TestRetryWhileOutput { result, call_count, gone_check_count, notify_count }
}

/// Output of [`test_retry_operation_indefinitely`].
struct TestIndefiniteRetryOutput<T, E> {
    result: Result<T, IndefiniteRetryOperationError<E>>,
    call_count: usize,
    notify_count: usize,
}

/// Wraps [`retry_operation_indefinitely`] with automatic counter tracking
/// and notification-attempt assertions.
async fn test_retry_operation_indefinitely<T, E, F, Fut>(
    backoff: IndefiniteBackoffParams,
    mut operation: F,
) -> TestIndefiniteRetryOutput<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Error<E>>>,
{
    let mut call_count = 0usize;
    let mut notify_count = 0usize;

    let result = retry_operation_indefinitely(
        backoff,
        || {
            call_count += 1;
            operation()
        },
        |notification| {
            let expected = notify_count + 1;
            assert_eq!(
                notification.attempt, expected,
                "expected attempt {expected}, got {}",
                notification.attempt,
            );
            notify_count += 1;
        },
    )
    .await;

    TestIndefiniteRetryOutput { result, call_count, notify_count }
}

/// Output of [`test_retry_operation_while_indefinitely`].
struct TestIndefiniteRetryWhileOutput<T, E, GoneErr = Infallible> {
    result: Result<T, IndefiniteRetryOperationWhileError<E, GoneErr>>,
    call_count: usize,
    gone_check_count: usize,
    notify_count: usize,
}

/// Wraps [`retry_operation_while_indefinitely`] with automatic counter
/// tracking and notification-attempt assertions.
async fn test_retry_operation_while_indefinitely<
    T,
    E,
    GoneErr,
    F,
    Fut,
    GF,
    GFut,
>(
    backoff: IndefiniteBackoffParams,
    mut operation: F,
    mut gone_check: GF,
) -> TestIndefiniteRetryWhileOutput<T, E, GoneErr>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Error<E>>>,
    GF: FnMut() -> GFut,
    GFut: Future<Output = Result<GoneCheckResult, GoneErr>>,
{
    let mut call_count = 0usize;
    let mut gone_check_count = 0usize;
    let mut notify_count = 0usize;

    let result = retry_operation_while_indefinitely(
        backoff,
        || {
            call_count += 1;
            operation()
        },
        || {
            gone_check_count += 1;
            gone_check()
        },
        |notification| {
            let expected = notify_count + 1;
            assert_eq!(
                notification.attempt, expected,
                "expected attempt {expected}, got {}",
                notification.attempt,
            );
            notify_count += 1;
        },
    )
    .await;

    TestIndefiniteRetryWhileOutput {
        result,
        call_count,
        gone_check_count,
        notify_count,
    }
}

// ---
// retry_operation
// ---

#[tokio::test]
async fn retry_op_succeeds_immediately() {
    let output = test_retry_operation(test_backoff(3), || async {
        Ok::<_, Error<()>>(42)
    })
    .await;

    assert_eq!(output.result.unwrap(), 42);
    assert_eq!(output.call_count, 1);
    assert_eq!(output.notify_count, 0);
}

#[tokio::test]
async fn retry_op_retries_transient_then_succeeds() {
    let mut attempt = 0usize;
    let output = test_retry_operation(test_backoff(5), || {
        let a = attempt;
        attempt += 1;
        async move { if a < 3 { Err(retryable_error()) } else { Ok(42u32) } }
    })
    .await;

    assert_eq!(output.result.unwrap(), 42);
    // 3 transient failures + 1 success = 4 total calls.
    assert_eq!(output.call_count, 4);
    assert_eq!(output.notify_count, 3);
}

#[tokio::test]
async fn retry_op_returns_permanent_error_immediately() {
    let output: TestRetryOutput<u32, ()> =
        test_retry_operation(test_backoff(5), || async {
            Err(permanent_error())
        })
        .await;

    let err = output.result.unwrap_err();
    assert!(matches!(err.kind, RetryOperationErrorKind::OperationError(_)),);
    // Non-retryable: no retries, only the initial attempt.
    assert_eq!(output.call_count, 1);
    assert_eq!(output.notify_count, 0);
}

#[tokio::test]
async fn retry_op_exhausts_retries() {
    let output: TestRetryOutput<u32, ()> =
        test_retry_operation(test_backoff(3), || async {
            Err(retryable_error())
        })
        .await;

    let err = output.result.unwrap_err();
    assert!(
        matches!(err.kind, RetryOperationErrorKind::RetriesExhausted(_)),
        "expected RetriesExhausted, got {err:?}",
    );
    assert_eq!(err.attempt, 4, "expected 1-indexed attempt 4");
    // 1 initial attempt + 3 retries = 4 total calls.
    assert_eq!(output.call_count, 4);
    assert_eq!(output.notify_count, 3);
}

#[tokio::test]
async fn retry_op_zero_retries_returns_first_error() {
    let output: TestRetryOutput<u32, ()> =
        test_retry_operation(test_backoff(0), || async {
            Err(retryable_error())
        })
        .await;

    let err = output.result.unwrap_err();
    assert!(
        matches!(err.kind, RetryOperationErrorKind::RetriesExhausted(_)),
        "expected RetriesExhausted, got {err:?}",
    );
    assert_eq!(err.attempt, 1, "expected 1-indexed attempt 1");
    // Zero retries: only the initial attempt.
    assert_eq!(output.call_count, 1);
    assert_eq!(output.notify_count, 0);
}

#[tokio::test]
async fn retry_op_different_retryable_statuses() {
    let mut attempt = 0usize;
    let output = test_retry_operation(test_backoff(5), || {
        let a = attempt;
        attempt += 1;
        async move {
            match a {
                0 => Err(Error::ErrorResponse(ResponseValue::new(
                    (),
                    StatusCode::TOO_MANY_REQUESTS,
                    HeaderMap::new(),
                ))),
                1 => Err(Error::ErrorResponse(ResponseValue::new(
                    (),
                    StatusCode::BAD_GATEWAY,
                    HeaderMap::new(),
                ))),
                2 => Err(Error::ErrorResponse(ResponseValue::new(
                    (),
                    StatusCode::GATEWAY_TIMEOUT,
                    HeaderMap::new(),
                ))),
                _ => Ok::<_, Error<()>>(42),
            }
        }
    })
    .await;

    assert_eq!(output.result.unwrap(), 42);
    assert_eq!(output.call_count, 4);
    assert_eq!(output.notify_count, 3);
}

// ---
// retry_operation_while
// ---

#[tokio::test]
async fn retry_while_succeeds_with_available_target() {
    let output = test_retry_operation_while(
        test_backoff(3),
        || async { Ok::<_, Error<()>>(42u32) },
        || async { Ok::<_, Infallible>(GoneCheckResult::StillAvailable) },
    )
    .await;

    assert_eq!(output.result.unwrap(), 42);
    assert_eq!(output.call_count, 1);
    assert_eq!(output.gone_check_count, 1);
    assert_eq!(output.notify_count, 0);
}

#[tokio::test]
async fn retry_while_aborts_when_target_is_gone() {
    let output: TestRetryWhileOutput<u32, ()> = test_retry_operation_while(
        test_backoff(3),
        || async { Ok::<_, Error<()>>(42u32) },
        || async { Ok::<_, Infallible>(GoneCheckResult::Gone) },
    )
    .await;

    assert!(output.result.unwrap_err().is_gone());
    // The operation closure is not invoked when the gone check
    // short-circuits.
    assert_eq!(output.call_count, 0);
    assert_eq!(output.gone_check_count, 1);
    assert_eq!(output.notify_count, 0);
}

#[tokio::test]
async fn retry_while_aborts_on_gone_check_error() {
    let output: TestRetryWhileOutput<u32, (), String> =
        test_retry_operation_while(
            test_backoff(3),
            || async { Ok::<_, Error<()>>(42u32) },
            || async { Err::<GoneCheckResult, _>("check failed".to_string()) },
        )
        .await;

    let err = output.result.unwrap_err();
    assert!(matches!(
        err.kind,
        RetryOperationWhileErrorKind::GoneCheckError(ref e) if e == "check failed"
    ));
    assert_eq!(output.call_count, 0);
    assert_eq!(output.gone_check_count, 1);
    assert_eq!(output.notify_count, 0);
}

#[tokio::test]
async fn retry_while_retries_transient_errors() {
    let mut attempt = 0usize;
    let output = test_retry_operation_while(
        test_backoff(5),
        || {
            let a = attempt;
            attempt += 1;
            async move {
                if a < 2 { Err(retryable_error()) } else { Ok(42u32) }
            }
        },
        || async { Ok::<_, Infallible>(GoneCheckResult::StillAvailable) },
    )
    .await;

    assert_eq!(output.result.unwrap(), 42);
    assert_eq!(output.call_count, 3);
    // The gone check is called before each operation attempt.
    assert_eq!(output.gone_check_count, 3);
    assert_eq!(output.notify_count, 2);
}

#[tokio::test]
async fn retry_while_returns_permanent_operation_error() {
    let output: TestRetryWhileOutput<u32, ()> = test_retry_operation_while(
        test_backoff(5),
        || async { Err(permanent_error()) },
        || async { Ok::<_, Infallible>(GoneCheckResult::StillAvailable) },
    )
    .await;

    let err = output.result.unwrap_err();
    assert!(matches!(
        err.kind,
        RetryOperationWhileErrorKind::OperationError(_),
    ));
    // Permanent error: no retries.
    assert_eq!(output.call_count, 1);
    assert_eq!(output.gone_check_count, 1);
    assert_eq!(output.notify_count, 0);
}

#[tokio::test]
async fn retry_while_exhausts_retries() {
    let output: TestRetryWhileOutput<u32, ()> = test_retry_operation_while(
        test_backoff(3),
        || async { Err(retryable_error()) },
        || async { Ok::<_, Infallible>(GoneCheckResult::StillAvailable) },
    )
    .await;

    let err = output.result.unwrap_err();
    assert!(
        matches!(err.kind, RetryOperationWhileErrorKind::RetriesExhausted(_),),
        "expected RetriesExhausted, got {err:?}"
    );
    assert_eq!(err.attempt, 4, "expected 1-indexed attempt 4");
    // 1 initial attempt + 3 retries = 4 total calls.
    assert_eq!(output.call_count, 4);
    // Gone check runs before each operation attempt.
    assert_eq!(output.gone_check_count, 4);
    assert_eq!(output.notify_count, 3);
}

#[tokio::test]
async fn retry_while_target_goes_away_during_retries() {
    let mut check_num = 0usize;
    let output: TestRetryWhileOutput<u32, ()> = test_retry_operation_while(
        test_backoff(5),
        // Always return a retryable error so retries continue.
        || async { Err::<u32, _>(retryable_error()) },
        || {
            let c = check_num;
            check_num += 1;
            async move {
                if c < 2 {
                    Ok::<_, Infallible>(GoneCheckResult::StillAvailable)
                } else {
                    Ok::<_, Infallible>(GoneCheckResult::Gone)
                }
            }
        },
    )
    .await;

    let err = output.result.unwrap_err();
    assert!(err.is_gone());
    // Gone check ran 3 times: twice StillAvailable, then Gone.
    // The operation is only invoked when the gone check returns
    // StillAvailable, so call_count is 2 (not 3).
    assert_eq!(output.call_count, 2);
    assert_eq!(output.gone_check_count, 3);
    assert_eq!(output.notify_count, 2);
}

// ---
// RetryOperationError helpers
// ---

#[test]
fn op_is_not_found_for_404_operation_error() {
    let err: RetryOperationError<()> = RetryOperationError {
        attempt: 0,
        kind: RetryOperationErrorKind::OperationError(not_found_error()),
    };
    assert!(err.is_not_found());
}

#[test]
fn op_is_not_found_returns_false_for_other_status() {
    let err: RetryOperationError<()> = RetryOperationError {
        attempt: 0,
        kind: RetryOperationErrorKind::OperationError(Error::ErrorResponse(
            ResponseValue::new(
                (),
                StatusCode::INTERNAL_SERVER_ERROR,
                HeaderMap::new(),
            ),
        )),
    };
    assert!(!err.is_not_found());
}

#[test]
fn op_is_not_found_returns_false_for_retries_exhausted() {
    let err: RetryOperationError<()> = RetryOperationError {
        attempt: 0,
        kind: RetryOperationErrorKind::RetriesExhausted(retryable_error()),
    };
    assert!(!err.is_not_found());
}

// ---
// RetryOperationWhileError helpers
// ---

#[test]
fn is_gone_returns_true_for_gone_variant() {
    let err: RetryOperationWhileError<()> = RetryOperationWhileError {
        attempt: 0,
        kind: RetryOperationWhileErrorKind::Gone,
    };
    assert!(err.is_gone());
}

#[test]
fn is_gone_returns_false_for_operation_error() {
    let err: RetryOperationWhileError<()> = RetryOperationWhileError {
        attempt: 0,
        kind: RetryOperationWhileErrorKind::OperationError(permanent_error()),
    };
    assert!(!err.is_gone());
}

#[test]
fn is_gone_returns_false_for_gone_check_error() {
    let err: RetryOperationWhileError<(), String> = RetryOperationWhileError {
        attempt: 0,
        kind: RetryOperationWhileErrorKind::GoneCheckError("oops".to_string()),
    };
    assert!(!err.is_gone());
}

#[test]
fn is_gone_returns_false_for_retries_exhausted() {
    let err: RetryOperationWhileError<()> = RetryOperationWhileError {
        attempt: 0,
        kind: RetryOperationWhileErrorKind::RetriesExhausted(retryable_error()),
    };
    assert!(!err.is_gone());
}

#[test]
fn is_not_found_for_404() {
    let err: RetryOperationWhileError<()> = RetryOperationWhileError {
        attempt: 0,
        kind: RetryOperationWhileErrorKind::OperationError(not_found_error()),
    };
    assert!(err.is_not_found());
}

#[test]
fn is_not_found_returns_false_for_other_status() {
    let err: RetryOperationWhileError<()> = RetryOperationWhileError {
        attempt: 0,
        kind: RetryOperationWhileErrorKind::OperationError(
            Error::ErrorResponse(ResponseValue::new(
                (),
                StatusCode::INTERNAL_SERVER_ERROR,
                HeaderMap::new(),
            )),
        ),
    };
    assert!(!err.is_not_found());
}

#[test]
fn is_not_found_returns_false_for_gone() {
    let err: RetryOperationWhileError<()> = RetryOperationWhileError {
        attempt: 0,
        kind: RetryOperationWhileErrorKind::Gone,
    };
    assert!(!err.is_not_found());
}

#[test]
fn is_not_found_returns_false_for_gone_check_error() {
    let err: RetryOperationWhileError<(), String> = RetryOperationWhileError {
        attempt: 0,
        kind: RetryOperationWhileErrorKind::GoneCheckError("oops".to_string()),
    };
    assert!(!err.is_not_found());
}

#[test]
fn is_not_found_returns_false_for_retries_exhausted() {
    let err: RetryOperationWhileError<()> = RetryOperationWhileError {
        attempt: 0,
        kind: RetryOperationWhileErrorKind::RetriesExhausted(retryable_error()),
    };
    assert!(!err.is_not_found());
}

// ---
// retry_operation_indefinitely
// ---

#[tokio::test(start_paused = true)]
async fn retry_op_indef_succeeds_immediately() {
    let output = test_retry_operation_indefinitely(
        default_indefinite_retry_policy(),
        || async { Ok::<_, Error<()>>(42) },
    )
    .await;

    assert_eq!(output.result.unwrap(), 42);
    assert_eq!(output.call_count, 1);
    assert_eq!(output.notify_count, 0);
}

#[tokio::test(start_paused = true)]
async fn retry_op_indef_retries_transient_then_succeeds() {
    let mut attempt = 0usize;
    let output =
        test_retry_operation_indefinitely(default_indefinite_retry_policy(), || {
            let a = attempt;
            attempt += 1;
            async move {
                if a < 3 { Err(retryable_error()) } else { Ok(42u32) }
            }
        })
        .await;

    assert_eq!(output.result.unwrap(), 42);
    // 3 transient failures + 1 success = 4 total calls.
    assert_eq!(output.call_count, 4);
    assert_eq!(output.notify_count, 3);
}

#[tokio::test(start_paused = true)]
async fn retry_op_indef_returns_permanent_error_immediately() {
    let output: TestIndefiniteRetryOutput<u32, ()> =
        test_retry_operation_indefinitely(
            default_indefinite_retry_policy(),
            || async { Err(permanent_error()) },
        )
        .await;

    let err = output.result.unwrap_err();
    // The error always wraps a non-retryable progenitor_client::Error.
    assert!(!err.error.is_retryable());
    // Non-retryable: no retries, only the initial attempt.
    assert_eq!(output.call_count, 1);
    assert_eq!(output.notify_count, 0);
}

#[tokio::test(start_paused = true)]
async fn retry_op_indef_different_retryable_statuses() {
    let mut attempt = 0usize;
    let output = test_retry_operation_indefinitely(
        default_indefinite_retry_policy(),
        || {
            let a = attempt;
            attempt += 1;
            async move {
                match a {
                    0 => Err(Error::ErrorResponse(ResponseValue::new(
                        (),
                        StatusCode::TOO_MANY_REQUESTS,
                        HeaderMap::new(),
                    ))),
                    1 => Err(Error::ErrorResponse(ResponseValue::new(
                        (),
                        StatusCode::BAD_GATEWAY,
                        HeaderMap::new(),
                    ))),
                    2 => Err(Error::ErrorResponse(ResponseValue::new(
                        (),
                        StatusCode::GATEWAY_TIMEOUT,
                        HeaderMap::new(),
                    ))),
                    _ => Ok::<_, Error<()>>(42),
                }
            }
        },
    )
    .await;

    assert_eq!(output.result.unwrap(), 42);
    assert_eq!(output.call_count, 4);
    assert_eq!(output.notify_count, 3);
}

// Verify that the indefinite backoff iterator is truly unbounded. Without
// the `.without_max_times()` and `.with_total_delay(None)` calls in
// `IndefiniteBackoffParams::build`, backon would default to 3 retries and
// the iterator would be exhausted (causing a panic).
const MIN_INDEFINITE_ITERATIONS: usize = 32_768;

#[tokio::test(start_paused = true)]
async fn indefinite_backoff_produces_at_least_32768_delays() {
    let mut attempt = 0usize;
    let output = test_retry_operation_indefinitely(
        IndefiniteBackoffParams {
            factor: 2.0,
            min_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
            jitter: false,
        },
        || {
            let a = attempt;
            attempt += 1;
            async move {
                if a < MIN_INDEFINITE_ITERATIONS {
                    Err(retryable_error())
                } else {
                    Ok(())
                }
            }
        },
    )
    .await;

    assert!(output.result.is_ok());
    assert_eq!(output.call_count, MIN_INDEFINITE_ITERATIONS + 1);
    assert_eq!(output.notify_count, MIN_INDEFINITE_ITERATIONS);
}

// ---
// retry_operation_while_indefinitely
// ---

#[tokio::test(start_paused = true)]
async fn retry_while_indef_succeeds_with_available_target() {
    let output = test_retry_operation_while_indefinitely(
        default_indefinite_retry_policy(),
        || async { Ok::<_, Error<()>>(42u32) },
        || async { Ok::<_, Infallible>(GoneCheckResult::StillAvailable) },
    )
    .await;

    assert_eq!(output.result.unwrap(), 42);
    assert_eq!(output.call_count, 1);
    assert_eq!(output.gone_check_count, 1);
    assert_eq!(output.notify_count, 0);
}

#[tokio::test(start_paused = true)]
async fn retry_while_indef_aborts_when_target_is_gone() {
    let output: TestIndefiniteRetryWhileOutput<u32, ()> =
        test_retry_operation_while_indefinitely(
            default_indefinite_retry_policy(),
            || async { Ok::<_, Error<()>>(42u32) },
            || async { Ok::<_, Infallible>(GoneCheckResult::Gone) },
        )
        .await;

    assert!(output.result.unwrap_err().is_gone());
    // The operation closure is not invoked when the gone check
    // short-circuits.
    assert_eq!(output.call_count, 0);
    assert_eq!(output.gone_check_count, 1);
    assert_eq!(output.notify_count, 0);
}

#[tokio::test(start_paused = true)]
async fn retry_while_indef_aborts_on_gone_check_error() {
    let output: TestIndefiniteRetryWhileOutput<u32, (), String> =
        test_retry_operation_while_indefinitely(
            default_indefinite_retry_policy(),
            || async { Ok::<_, Error<()>>(42u32) },
            || async { Err::<GoneCheckResult, _>("check failed".to_string()) },
        )
        .await;

    let err = output.result.unwrap_err();
    assert!(matches!(
        err.kind,
        IndefiniteRetryOperationWhileErrorKind::GoneCheckError(ref e)
            if e == "check failed"
    ));
    assert_eq!(output.call_count, 0);
    assert_eq!(output.gone_check_count, 1);
    assert_eq!(output.notify_count, 0);
}

#[tokio::test(start_paused = true)]
async fn retry_while_indef_retries_transient_errors() {
    let mut attempt = 0usize;
    let output = test_retry_operation_while_indefinitely(
        default_indefinite_retry_policy(),
        || {
            let a = attempt;
            attempt += 1;
            async move {
                if a < 2 { Err(retryable_error()) } else { Ok(42u32) }
            }
        },
        || async { Ok::<_, Infallible>(GoneCheckResult::StillAvailable) },
    )
    .await;

    assert_eq!(output.result.unwrap(), 42);
    assert_eq!(output.call_count, 3);
    // The gone check is called before each operation attempt.
    assert_eq!(output.gone_check_count, 3);
    assert_eq!(output.notify_count, 2);
}

#[tokio::test(start_paused = true)]
async fn retry_while_indef_returns_permanent_operation_error() {
    let output: TestIndefiniteRetryWhileOutput<u32, ()> =
        test_retry_operation_while_indefinitely(
            default_indefinite_retry_policy(),
            || async { Err(permanent_error()) },
            || async { Ok::<_, Infallible>(GoneCheckResult::StillAvailable) },
        )
        .await;

    let err = output.result.unwrap_err();
    assert!(matches!(
        err.kind,
        IndefiniteRetryOperationWhileErrorKind::OperationError(_),
    ));
    // Permanent error: no retries.
    assert_eq!(output.call_count, 1);
    assert_eq!(output.gone_check_count, 1);
    assert_eq!(output.notify_count, 0);
}

#[tokio::test(start_paused = true)]
async fn retry_while_indef_target_goes_away_during_retries() {
    let mut check_num = 0usize;
    let output: TestIndefiniteRetryWhileOutput<u32, ()> =
        test_retry_operation_while_indefinitely(
            default_indefinite_retry_policy(),
            // Always return a retryable error so retries continue.
            || async { Err::<u32, _>(retryable_error()) },
            || {
                let c = check_num;
                check_num += 1;
                async move {
                    if c < 2 {
                        Ok::<_, Infallible>(GoneCheckResult::StillAvailable)
                    } else {
                        Ok::<_, Infallible>(GoneCheckResult::Gone)
                    }
                }
            },
        )
        .await;

    let err = output.result.unwrap_err();
    assert!(err.is_gone());
    // Gone check ran 3 times: twice StillAvailable, then Gone.
    // The operation is only invoked when the gone check returns
    // StillAvailable, so call_count is 2 (not 3).
    assert_eq!(output.call_count, 2);
    assert_eq!(output.gone_check_count, 3);
    assert_eq!(output.notify_count, 2);
}

// ---
// IndefiniteRetryOperationError helpers
// ---

#[test]
fn indef_op_is_not_found_for_404() {
    let err: IndefiniteRetryOperationError<()> =
        IndefiniteRetryOperationError { attempt: 1, error: not_found_error() };
    assert!(err.is_not_found());
}

#[test]
fn indef_op_is_not_found_returns_false_for_other_status() {
    let err: IndefiniteRetryOperationError<()> =
        IndefiniteRetryOperationError {
            attempt: 1,
            error: Error::ErrorResponse(ResponseValue::new(
                (),
                StatusCode::INTERNAL_SERVER_ERROR,
                HeaderMap::new(),
            )),
        };
    assert!(!err.is_not_found());
}

// ---
// IndefiniteRetryOperationWhileError helpers
// ---

#[test]
fn indef_while_is_gone_returns_true_for_gone_variant() {
    let err: IndefiniteRetryOperationWhileError<()> =
        IndefiniteRetryOperationWhileError {
            attempt: 0,
            kind: IndefiniteRetryOperationWhileErrorKind::Gone,
        };
    assert!(err.is_gone());
}

#[test]
fn indef_while_is_gone_returns_false_for_operation_error() {
    let err: IndefiniteRetryOperationWhileError<()> =
        IndefiniteRetryOperationWhileError {
            attempt: 0,
            kind: IndefiniteRetryOperationWhileErrorKind::OperationError(
                permanent_error(),
            ),
        };
    assert!(!err.is_gone());
}

#[test]
fn indef_while_is_gone_returns_false_for_gone_check_error() {
    let err: IndefiniteRetryOperationWhileError<(), String> =
        IndefiniteRetryOperationWhileError {
            attempt: 0,
            kind: IndefiniteRetryOperationWhileErrorKind::GoneCheckError(
                "oops".to_string(),
            ),
        };
    assert!(!err.is_gone());
}

#[test]
fn indef_while_is_not_found_for_404() {
    let err: IndefiniteRetryOperationWhileError<()> =
        IndefiniteRetryOperationWhileError {
            attempt: 0,
            kind: IndefiniteRetryOperationWhileErrorKind::OperationError(
                not_found_error(),
            ),
        };
    assert!(err.is_not_found());
}

#[test]
fn indef_while_is_not_found_returns_false_for_other_status() {
    let err: IndefiniteRetryOperationWhileError<()> =
        IndefiniteRetryOperationWhileError {
            attempt: 0,
            kind: IndefiniteRetryOperationWhileErrorKind::OperationError(
                Error::ErrorResponse(ResponseValue::new(
                    (),
                    StatusCode::INTERNAL_SERVER_ERROR,
                    HeaderMap::new(),
                )),
            ),
        };
    assert!(!err.is_not_found());
}

#[test]
fn indef_while_is_not_found_returns_false_for_gone() {
    let err: IndefiniteRetryOperationWhileError<()> =
        IndefiniteRetryOperationWhileError {
            attempt: 0,
            kind: IndefiniteRetryOperationWhileErrorKind::Gone,
        };
    assert!(!err.is_not_found());
}

#[test]
fn indef_while_is_not_found_returns_false_for_gone_check_error() {
    let err: IndefiniteRetryOperationWhileError<(), String> =
        IndefiniteRetryOperationWhileError {
            attempt: 0,
            kind: IndefiniteRetryOperationWhileErrorKind::GoneCheckError(
                "oops".to_string(),
            ),
        };
    assert!(!err.is_not_found());
}

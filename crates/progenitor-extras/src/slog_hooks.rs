//! [`ClientHooks`] that log requests and responses via [`slog`].
//!
//! Progenitor-generated clients can customize their behavior by implementing
//! [`ClientHooks`]. A common pattern is to use a [`slog::Logger`] as the
//! client's `inner_type`, and to log each request and response at debug level.
//! This module provides that implementation, so that it doesn't have to be
//! copied into every client crate.
//!
//! The easiest way to use this module is via the [`impl_slog_client_hooks!`]
//! macro:
//!
//! ```
//! progenitor::generate_api!(
//!     spec = "tests/data/widgets.json",
//!     inner_type = slog::Logger,
//! );
//!
//! progenitor_extras::slog_hooks::impl_slog_client_hooks!(Client);
//! # fn main() {}
//! ```
//!
//! For clients that need to do more in their hooks, the [`log_request!`] and
//! [`log_response!`] macros can be used directly from a hand-written
//! [`ClientHooks`] implementation.
//!
//! [`ClientHooks`]: progenitor_client::ClientHooks

/// Logs an outgoing request at debug level.
///
/// This is meant to be used from [`ClientHooks::pre`].
///
/// This is a macro rather than a function so that slog attributes the module,
/// file, and line to the caller rather than to `progenitor-extras`. (This lets
/// `slog-envlogger` module filters work correctly.)
///
/// # Examples
///
/// A hand-written `ClientHooks::pre` that adds a header, then logs the request
/// (a pattern that the [`impl_slog_client_hooks!`] macro can't express
/// directly):
///
/// ```
/// use progenitor_client::{ClientHooks, ClientInfo, OperationInfo};
/// use progenitor_extras::slog_hooks::log_request;
/// use reqwest::header::HeaderValue;
///
/// # mod api {
/// #     progenitor::generate_api!(
/// #         spec = "tests/data/widgets.json",
/// #         inner_type = slog::Logger,
/// #     );
/// # }
/// # use api::Client;
/// #
/// impl ClientHooks<slog::Logger> for Client {
///     async fn pre<E>(
///         &self,
///         request: &mut reqwest::Request,
///         info: &OperationInfo,
///     ) -> Result<(), progenitor_client::Error<E>> {
///         request
///             .headers_mut()
///             .insert("x-caller", HeaderValue::from_static("my-service"));
///         log_request!(self.inner(), request, info);
///         Ok(())
///     }
/// }
/// #
/// # fn main() {}
/// ```
///
/// [`ClientHooks::pre`]: progenitor_client::ClientHooks::pre
#[doc(inline)]
pub use crate::__slog_hooks_log_request as log_request;

#[doc(hidden)]
#[macro_export]
macro_rules! __slog_hooks_log_request {
    ($log:expr, $request:expr, $info:expr $(,)?) => {
        // This is a somewhat strange way to write Rust, but the benefit is we
        // get type coercion/reborrowing, and each argument is bound to a name
        // exactly once (so e.g. `$request` isn't evaluated multiple times).
        match (
            ::std::convert::identity::<&$crate::__private::slog::Logger>($log),
            ::std::convert::identity::<&$crate::__private::reqwest::Request>(
                $request,
            ),
            ::std::convert::identity::<
                &$crate::__private::progenitor_client::OperationInfo,
            >($info),
        ) {
            (log, request, info) => {
                $crate::__private::slog::debug!(
                    log,
                    "client request";
                    "operation_id" => info.operation_id,
                    "method" => %request.method(),
                    "uri" => %request.url(),
                    "body" => ?&request.body(),
                );
            }
        }
    };
}

/// Logs the result of a request at debug level.
///
/// This is meant to be used from [`ClientHooks::post`].
///
/// This is a macro rather than a function so that slog attributes the module,
/// file, and line to the caller rather than to `progenitor-extras`. (This lets
/// `slog-envlogger` module filters work correctly.)
///
/// # Examples
///
/// A hand-written `ClientHooks::post` that logs the response, and warns on
/// transport failures (a pattern that the [`impl_slog_client_hooks!`] macro
/// can't express directly):
///
/// ```
/// use progenitor_client::{ClientHooks, ClientInfo, OperationInfo};
/// use progenitor_extras::slog_hooks::log_response;
///
/// # mod api {
/// #     progenitor::generate_api!(
/// #         spec = "tests/data/widgets.json",
/// #         inner_type = slog::Logger,
/// #     );
/// # }
/// # use api::Client;
/// #
/// impl ClientHooks<slog::Logger> for Client {
///     async fn post<E>(
///         &self,
///         result: &reqwest::Result<reqwest::Response>,
///         info: &OperationInfo,
///     ) -> Result<(), progenitor_client::Error<E>> {
///         log_response!(self.inner(), result, info);
///         if let Err(error) = result {
///             slog::warn!(
///                 self.inner(),
///                 "request failed";
///                 "operation_id" => info.operation_id,
///                 "error" => %error,
///             );
///         }
///         Ok(())
///     }
/// }
/// #
/// # fn main() {}
/// ```
///
/// [`ClientHooks::post`]: progenitor_client::ClientHooks::post
#[doc(inline)]
pub use crate::__slog_hooks_log_response as log_response;

#[doc(hidden)]
#[macro_export]
macro_rules! __slog_hooks_log_response {
    ($log:expr, $result:expr, $info:expr $(,)?) => {
        // See log_request for why this is so strange.
        match (
            ::std::convert::identity::<&$crate::__private::slog::Logger>($log),
            ::std::convert::identity::<
                &$crate::__private::reqwest::Result<
                    $crate::__private::reqwest::Response,
                >,
            >($result),
            ::std::convert::identity::<
                &$crate::__private::progenitor_client::OperationInfo,
            >($info),
        ) {
            (log, result, info) => {
                $crate::__private::slog::debug!(
                    log,
                    "client response";
                    "operation_id" => info.operation_id,
                    "result" => ?result,
                );
            }
        }
    };
}

/// Implements [`ClientHooks`] for a Progenitor-generated client, logging
/// requests and responses via [`slog`].
///
/// The client must have been generated with `inner_type = slog::Logger`.
/// Requests are logged with [`log_request!`], and responses with
/// [`log_response!`].
///
/// This macro must be invoked in the crate that defines the client. This is an
/// orphan rule requirement.
///
/// # Examples
///
/// ```
/// progenitor::generate_api!(
///     spec = "tests/data/widgets.json",
///     inner_type = slog::Logger,
/// );
///
/// progenitor_extras::slog_hooks::impl_slog_client_hooks!(Client);
/// # fn main() {}
/// ```
///
/// [`ClientHooks`]: progenitor_client::ClientHooks
#[doc(inline)]
pub use crate::__slog_hooks_impl_slog_client_hooks as impl_slog_client_hooks;

#[doc(hidden)]
#[macro_export]
macro_rules! __slog_hooks_impl_slog_client_hooks {
    ($client:ty $(,)?) => {
        impl
            $crate::__private::progenitor_client::ClientHooks<
                $crate::__private::slog::Logger,
            > for $client
        {
            async fn pre<E>(
                &self,
                request: &mut $crate::__private::reqwest::Request,
                info: &$crate::__private::progenitor_client::OperationInfo,
            ) -> ::std::result::Result<
                (),
                $crate::__private::progenitor_client::Error<E>,
            > {
                $crate::slog_hooks::log_request!(
                    $crate::__private::progenitor_client::ClientInfo::<
                        $crate::__private::slog::Logger,
                    >::inner(self),
                    request,
                    info,
                );
                ::std::result::Result::Ok(())
            }

            async fn post<E>(
                &self,
                result: &$crate::__private::reqwest::Result<
                    $crate::__private::reqwest::Response,
                >,
                info: &$crate::__private::progenitor_client::OperationInfo,
            ) -> ::std::result::Result<
                (),
                $crate::__private::progenitor_client::Error<E>,
            > {
                $crate::slog_hooks::log_response!(
                    $crate::__private::progenitor_client::ClientInfo::<
                        $crate::__private::slog::Logger,
                    >::inner(self),
                    result,
                    info,
                );
                ::std::result::Result::Ok(())
            }
        }
    };
}

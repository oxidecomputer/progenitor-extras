use crate::widgets::{self, HangUpServer};
use http::StatusCode;
use httptest::{
    Expectation, Server, all_of,
    matchers::{contains, request},
};
use slog::{Drain, KV, Key, OwnedKVList, Record, Serializer};
use std::{
    fmt,
    sync::{Arc, Mutex},
};

// ---
// Helpers
// ---

/// A log record captured by [`CaptureDrain`].
#[derive(Debug)]
struct CapturedRecord {
    level: slog::Level,
    msg: String,
    module: String,
    file: String,
    kvs: Vec<(String, String)>,
}

impl CapturedRecord {
    fn get(&self, key: &str) -> Option<&str> {
        self.kvs.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
    }
}

/// A drain that captures all records into a shared vector.
#[derive(Clone, Default)]
struct CaptureDrain {
    records: Arc<Mutex<Vec<CapturedRecord>>>,
}

impl Drain for CaptureDrain {
    type Ok = ();
    type Err = slog::Never;

    fn log(
        &self,
        record: &Record<'_>,
        _values: &OwnedKVList,
    ) -> Result<(), slog::Never> {
        let mut serializer = CaptureSerializer::default();
        record.kv().serialize(record, &mut serializer).unwrap();
        self.records.lock().unwrap().push(CapturedRecord {
            level: record.level(),
            msg: record.msg().to_string(),
            module: record.module().to_string(),
            file: record.file().to_string(),
            kvs: serializer.kvs,
        });
        Ok(())
    }
}

#[derive(Default)]
struct CaptureSerializer {
    kvs: Vec<(String, String)>,
}

impl Serializer for CaptureSerializer {
    fn emit_arguments(
        &mut self,
        key: Key,
        val: &fmt::Arguments<'_>,
    ) -> slog::Result {
        self.kvs.push((key.to_string(), val.to_string()));
        Ok(())
    }
}

mod client {
    progenitor::generate_api!(
        spec = "tests/data/widgets.json",
        inner_type = slog::Logger,
    );

    progenitor_extras::slog_hooks::impl_slog_client_hooks!(Client);

    pub(super) const MODULE_PATH: &str = module_path!();
}

mod manual_client {
    use progenitor_client::{ClientHooks, ClientInfo, OperationInfo};
    use progenitor_extras::slog_hooks::{log_request, log_response};
    use reqwest::header::HeaderValue;

    mod api {
        progenitor::generate_api!(
            spec = "tests/data/widgets.json",
            inner_type = slog::Logger,
        );
    }
    pub(super) use api::Client;

    impl ClientHooks<slog::Logger> for Client {
        async fn pre<E>(
            &self,
            request: &mut reqwest::Request,
            info: &OperationInfo,
        ) -> Result<(), progenitor_client::Error<E>> {
            log_request!(self.inner(), request, info);
            // Mutating after logging ensures that log_request! reborrows the
            // `&mut` request rather than moving it.
            request
                .headers_mut()
                .insert("x-caller", HeaderValue::from_static("manual-client"));
            Ok(())
        }

        async fn post<E>(
            &self,
            result: &reqwest::Result<reqwest::Response>,
            info: &OperationInfo,
        ) -> Result<(), progenitor_client::Error<E>> {
            log_response!(self.inner(), result, info);
            Ok(())
        }
    }

    pub(super) const MODULE_PATH: &str = module_path!();
}

fn make_logger() -> (slog::Logger, CaptureDrain) {
    let drain = CaptureDrain::default();
    let log = slog::Logger::root(drain.clone().fuse(), slog::o!());
    (log, drain)
}

fn make_client(base_url: &str) -> (client::Client, CaptureDrain) {
    let (log, drain) = make_logger();
    (client::Client::new(base_url, log), drain)
}

// Records must be attributed to the module that invoked the macro, not to
// progenitor-extras, so that module-based filters work.
fn assert_caller_location(record: &CapturedRecord, module_path: &str) {
    assert_eq!(record.module, module_path);
    assert_eq!(record.file, file!());
}

fn assert_request_record(
    record: &CapturedRecord,
    module_path: &str,
    uri: &str,
) {
    assert_caller_location(record, module_path);
    assert_eq!(record.level, slog::Level::Debug);
    assert_eq!(record.msg, "client request");
    assert_eq!(record.get("operation_id"), Some("widget_get"));
    assert_eq!(record.get("method"), Some("GET"));
    assert_eq!(record.get("uri"), Some(uri));
    assert_eq!(record.get("body"), Some("None"));
}

fn response_record_result<'a>(
    record: &'a CapturedRecord,
    module_path: &str,
) -> &'a str {
    assert_caller_location(record, module_path);
    assert_eq!(record.level, slog::Level::Debug);
    assert_eq!(record.msg, "client response");
    assert_eq!(record.get("operation_id"), Some("widget_get"));
    record.get("result").expect("result key is present")
}

enum ExpectedResult {
    Response(StatusCode),
    TransportError,
}

fn assert_exchange_logged(
    drain: &CaptureDrain,
    module_path: &str,
    uri: &str,
    expected: ExpectedResult,
) {
    let records = drain.records.lock().unwrap();
    let [request, response] = records.as_slice() else {
        panic!("expected one request and one response record: {records:?}");
    };

    assert_request_record(request, module_path, uri);
    let result = response_record_result(response, module_path);
    match expected {
        ExpectedResult::Response(status) => {
            // We have Debug output here -- match as much of it as reasonable.
            // This isn't great (Debug output is unstable) but we don't have
            // structured errors so it's the best we can do.
            let prefix = format!(
                "Ok(Response {{ url: {uri:?}, status: {}, headers: ",
                status.as_u16(),
            );
            assert!(
                result.starts_with(&prefix),
                "result starts with {prefix:?}: {result}",
            );
        }
        ExpectedResult::TransportError => {
            assert!(result.starts_with("Err("), "result is Err: {result}");
        }
    }
}

// ---
// Tests
// ---

#[tokio::test]
async fn generated_client_logs_request_and_response() {
    let mut server = Server::run();
    server.expect(
        Expectation::matching(request::method_path("GET", "/widgets/w1"))
            .times(1)
            .respond_with(widgets::widget_response("w1", "sprocket")),
    );
    let base_url = widgets::base_url(&server);
    let (client, drain) = make_client(&base_url);

    let widget = client
        .widget_get("w1")
        .await
        .expect("widget_get succeeded")
        .into_inner();
    assert_eq!(widget.name, "sprocket");
    server.verify_and_clear();

    assert_exchange_logged(
        &drain,
        client::MODULE_PATH,
        &format!("{base_url}/widgets/w1"),
        ExpectedResult::Response(StatusCode::OK),
    );
}

#[tokio::test]
async fn generated_client_logs_connection_error() {
    let mut server = HangUpServer::start().await;
    let (client, drain) = make_client(&server.base_url());

    let error = client
        .widget_get("w1")
        .await
        .expect_err("server hung up without responding");
    assert_eq!(error.status(), None);
    assert_eq!(server.take_hang_up_count(), 1);

    assert_exchange_logged(
        &drain,
        client::MODULE_PATH,
        &format!("{}/widgets/w1", server.base_url()),
        ExpectedResult::TransportError,
    );
}

#[tokio::test]
async fn hand_written_hooks_log_request_and_response() {
    let mut server = Server::run();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("GET", "/widgets/w1"),
            request::headers(contains(("x-caller", "manual-client"))),
        ])
        .times(1)
        .respond_with(widgets::widget_response("w1", "sprocket")),
    );
    let base_url = widgets::base_url(&server);
    let (log, drain) = make_logger();
    let client = manual_client::Client::new(&base_url, log);

    let widget = client
        .widget_get("w1")
        .await
        .expect("widget_get succeeded")
        .into_inner();
    assert_eq!(widget.name, "sprocket");
    server.verify_and_clear();

    assert_exchange_logged(
        &drain,
        manual_client::MODULE_PATH,
        &format!("{base_url}/widgets/w1"),
        ExpectedResult::Response(StatusCode::OK),
    );
}

use http::StatusCode;
use httptest::responders::{ResponseBuilder, json_encoded, status_code};
use std::net::{Ipv4Addr, SocketAddr};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    sync::mpsc,
};

pub mod client {
    progenitor::generate_api!(spec = "tests/data/widgets.json");
}

pub fn base_url(server: &httptest::Server) -> String {
    format!("http://{}", server.addr())
}

pub fn widget_response(id: &str, name: &str) -> ResponseBuilder<String> {
    json_encoded(serde_json::json!({ "id": id, "name": name }))
}

// This body matches the Error schema in widgets.json, so that the generated
// client deserializes it into types::Error rather than failing with
// InvalidResponsePayload.
pub fn error_response(status: StatusCode) -> ResponseBuilder<String> {
    let body = serde_json::json!({
        "message": status.canonical_reason().unwrap_or("error"),
        "request_id": "test-request-id",
    });
    status_code(status.as_u16())
        .append_header("content-type", "application/json")
        .body(body.to_string())
}

/// A server that hangs up and produces a transport error.
pub struct HangUpServer {
    addr: SocketAddr,
    hang_ups: mpsc::UnboundedReceiver<()>,
}

impl HangUpServer {
    pub async fn start() -> Self {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bound listener to a loopback port");
        let addr = listener.local_addr().expect("listener has a local address");
        let (tx, hang_ups) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener
                    .accept()
                    .await
                    .expect("accepted connection from client");
                read_request_head(&mut stream).await;
                // Record the hang-up before closing.
                //
                // The client will only see the hang-up once the stream is
                // dropped, so the record is already guaranteed to be in the
                // channel when the client call returns.
                //
                // Ignore an error in tx.send() -- the send only fails if the
                // receiver is dropped, in which case nobody is counting the
                // number of hang-ups anymore and it doesn't matter.
                _ = tx.send(());
                drop(stream);
            }
        });

        Self { addr, hang_ups }
    }

    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Returns the number of hang-ups that have been recorded and resets the
    /// count to zero.
    pub fn take_hang_up_count(&mut self) -> usize {
        let mut count = 0;
        while let Ok(()) = self.hang_ups.try_recv() {
            count += 1;
        }
        count
    }
}

// We only support GET requests without a body, because the request must be
// fully read before the connection is closed. Closing a socket with unread data
// (e.g. a request body) can make the kernel send RST rather than FIN, and then
// the error the client sees depends on timing.
async fn read_request_head(stream: &mut TcpStream) {
    let mut head = Vec::new();
    while !head.windows(4).any(|window| window == b"\r\n\r\n") {
        let mut chunk = [0u8; 1024];
        let n =
            stream.read(&mut chunk).await.expect("read request from client");
        assert!(n > 0, "client closed connection before sending full headers");
        head.extend_from_slice(&chunk[..n]);
    }
    assert!(
        head.starts_with(b"GET "),
        "HangUpServer only supports GET requests, got: {}",
        String::from_utf8_lossy(&head),
    );
}

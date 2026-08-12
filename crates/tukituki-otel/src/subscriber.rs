//! Reconnecting client for the collector's notify socket.
//!
//! Port of Go's `internal/tui/otel_notify.go` `runOtelNotifySubscriber`:
//! dial the collector's Unix-domain socket, open a `Notifier/Subscribe`
//! stream, and forward every pushed [`ErrorEvent`] to the returned
//! channel. The dial is retried forever, so a TUI attached before the
//! collector is up — or across a collector restart — recovers
//! transparently.

use std::path::PathBuf;
use std::sync::mpsc;
use std::time::Duration;

use tonic::transport::Endpoint;

use crate::proto::notify_v1::notifier_client::NotifierClient;
use crate::proto::notify_v1::{ErrorEvent, SubscribeRequest};

/// Delay between dial/subscribe attempts while the collector is down.
const DIAL_RETRY: Duration = Duration::from_secs(1);

/// Delay before re-dialing after an established stream ends (collector
/// restart). Shorter than [`DIAL_RETRY`] so a bounce is picked up fast.
const STREAM_RETRY: Duration = Duration::from_millis(500);

/// Spawn a background thread that subscribes to the notify socket at
/// `socket` and forwards each received event to the returned channel.
///
/// The thread runs a single-threaded tokio runtime (tonic is async;
/// callers are not). It exits when the receiver is dropped — detected
/// on the first `send` after the drop, so a subscriber with no traffic
/// parks harmlessly until process exit.
pub fn spawn_error_subscriber(socket: PathBuf) -> mpsc::Receiver<ErrorEvent> {
    let (tx, rx) = mpsc::channel();
    let _ = std::thread::Builder::new()
        .name("tukituki-otel-subscriber".into())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(_) => return,
            };
            rt.block_on(subscriber_loop(socket, tx));
        });
    rx
}

async fn subscriber_loop(socket: PathBuf, tx: mpsc::Sender<ErrorEvent>) {
    loop {
        // tonic has no built-in UDS support, so a custom connector
        // converts each dial into a `TokioIo<UnixStream>`. The URI is
        // purely cosmetic (used for the HTTP/2 :authority header).
        let socket_for_dial = socket.clone();
        let Ok(endpoint) = Endpoint::try_from("http://[::]:50051") else {
            return;
        };
        let channel = endpoint
            .connect_with_connector(tower::service_fn(move |_: tonic::transport::Uri| {
                let path = socket_for_dial.clone();
                async move {
                    let stream = tokio::net::UnixStream::connect(&path).await?;
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                }
            }))
            .await;
        let channel = match channel {
            Ok(c) => c,
            Err(_) => {
                tokio::time::sleep(DIAL_RETRY).await;
                continue;
            }
        };

        let mut client = NotifierClient::new(channel);
        let mut stream = match client.subscribe(SubscribeRequest {}).await {
            Ok(resp) => resp.into_inner(),
            Err(_) => {
                tokio::time::sleep(DIAL_RETRY).await;
                continue;
            }
        };

        while let Ok(Some(ev)) = stream.message().await {
            if tx.send(ev).is_err() {
                // Receiver dropped — the TUI is gone.
                return;
            }
        }
        tokio::time::sleep(STREAM_RETRY).await;
    }
}

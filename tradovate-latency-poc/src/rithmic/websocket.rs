use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_tls_with_config,
    tungstenite::{Error as WsError, Message},
};

use crate::{config::WS_URL, models::ConnectionCounters, rithmic::protocol};

pub(crate) enum ConnectionRole {
    Discovery,
    Benchmark,
}

pub(crate) enum ReadEvent {
    Binary {
        received_at_instant: Instant,
        received_at_utc: DateTime<Utc>,
        template_id: i32,
        payload: Vec<u8>,
    },
    TimedOut,
    Closed(String),
    Ignored,
}

pub(crate) struct WsClient {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl WsClient {
    pub(crate) async fn connect(
        timeout: Duration,
        role: ConnectionRole,
        counters: &mut ConnectionCounters,
    ) -> Result<Self> {
        let result = tokio::time::timeout(
            timeout,
            connect_async_tls_with_config(WS_URL, None, true, None),
        )
        .await;
        let (socket, _) = match result {
            Err(_) => {
                counters.connect_failures += 1;
                counters.timeouts += 1;
                bail!(
                    "connection to {WS_URL} timed out after {}s",
                    timeout.as_secs()
                );
            }
            Ok(Err(error)) => {
                counters.connect_failures += 1;
                return Err(anyhow!(error).context(format!("failed to connect to {WS_URL}")));
            }
            Ok(Ok(value)) => value,
        };
        counters.connections_opened += 1;
        match role {
            ConnectionRole::Discovery => counters.discovery_connections += 1,
            ConnectionRole::Benchmark => counters.benchmark_connections += 1,
        }
        Ok(Self { socket })
    }

    pub(crate) async fn send<M: ProstMessage>(
        &mut self,
        message: &M,
        counters: &mut ConnectionCounters,
    ) -> Result<()> {
        self.socket
            .send(Message::Binary(message.encode_to_vec()))
            .await
            .inspect_err(|error| record_write_failure(error, counters))
            .context("R|Protocol WebSocket write failed")
    }

    pub(crate) async fn send_timed<M: ProstMessage>(
        &mut self,
        message: &M,
        counters: &mut ConnectionCounters,
    ) -> Result<Instant> {
        let payload = message.encode_to_vec();
        let sent_at = Instant::now();
        self.socket
            .send(Message::Binary(payload))
            .await
            .inspect_err(|error| record_write_failure(error, counters))
            .context("R|Protocol WebSocket write failed")?;
        Ok(sent_at)
    }

    pub(crate) async fn read(
        &mut self,
        wait: Duration,
        counters: &mut ConnectionCounters,
    ) -> Result<ReadEvent> {
        let message = match tokio::time::timeout(wait, self.socket.next()).await {
            Err(_) => return Ok(ReadEvent::TimedOut),
            Ok(None) => return Ok(ReadEvent::Closed("stream ended".to_owned())),
            Ok(Some(Err(error))) => {
                counters.read_errors += 1;
                if matches!(
                    error,
                    WsError::ConnectionClosed | WsError::AlreadyClosed | WsError::Io(_)
                ) {
                    counters.disconnects += 1;
                    counters.unexpected_closes += 1;
                }
                return Err(anyhow!(error).context("R|Protocol WebSocket read failed"));
            }
            Ok(Some(Ok(message))) => message,
        };

        match message {
            Message::Binary(payload) => {
                let received_at_instant = Instant::now();
                let received_at_utc = Utc::now();
                let template_id = protocol::template_id(&payload).inspect_err(|_| {
                    counters.protocol_errors += 1;
                })?;
                Ok(ReadEvent::Binary {
                    received_at_instant,
                    received_at_utc,
                    template_id,
                    payload,
                })
            }
            Message::Close(frame) => Ok(ReadEvent::Closed(
                frame
                    .map(|value| value.reason.to_string())
                    .unwrap_or_else(|| "close frame without reason".to_owned()),
            )),
            Message::Ping(payload) => {
                counters.websocket_pings_received += 1;
                self.socket
                    .send(Message::Pong(payload))
                    .await
                    .inspect_err(|_| counters.write_errors += 1)
                    .context("failed to answer WebSocket ping")?;
                Ok(ReadEvent::Ignored)
            }
            Message::Text(_) => {
                counters.protocol_errors += 1;
                Ok(ReadEvent::Ignored)
            }
            Message::Pong(_) | Message::Frame(_) => Ok(ReadEvent::Ignored),
        }
    }

    pub(crate) async fn close(mut self, counters: &mut ConnectionCounters) -> Result<()> {
        self.socket
            .close(None)
            .await
            .inspect_err(|_| counters.close_errors += 1)
            .context("failed to close R|Protocol WebSocket")?;
        counters.intentional_closes += 1;
        Ok(())
    }
}

pub(crate) fn record_unexpected_close(counters: &mut ConnectionCounters) {
    counters.disconnects += 1;
    counters.unexpected_closes += 1;
}

fn record_write_failure(error: &WsError, counters: &mut ConnectionCounters) {
    counters.write_errors += 1;
    if matches!(
        error,
        WsError::ConnectionClosed | WsError::AlreadyClosed | WsError::Io(_)
    ) {
        record_unexpected_close(counters);
    }
}

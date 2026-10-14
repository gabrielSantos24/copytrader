use std::time::{Duration, Instant};

use anyhow::{Result, bail};

use crate::{
    config::Config,
    models::ConnectionCounters,
    rithmic::{
        messages,
        pb::RequestHeartbeat,
        protocol,
        websocket::{ReadEvent, WsClient, record_unexpected_close},
    },
};

pub(crate) async fn run(
    ws: &mut WsClient,
    heartbeat_interval: Duration,
    config: &Config,
    counters: &mut ConnectionCounters,
) -> Result<()> {
    if config.stability_duration.is_zero() {
        return Ok(());
    }
    println!(
        "Monitoring connection stability for {} seconds",
        config.stability_duration.as_secs()
    );
    let deadline = Instant::now() + config.stability_duration;
    let keepalive_every = heartbeat_interval.mul_f64(0.8).max(Duration::from_secs(1));
    let mut next_keepalive = Instant::now() + keepalive_every;
    let mut sequence = 0_u64;

    loop {
        let now = Instant::now();
        if now >= deadline {
            return Ok(());
        }
        if now >= next_keepalive {
            sequence += 1;
            send_keepalive(ws, config, counters, &format!("stability-{sequence}")).await?;
            next_keepalive = Instant::now() + keepalive_every;
            continue;
        }

        let wait = next_keepalive
            .saturating_duration_since(now)
            .min(deadline.saturating_duration_since(now));
        match ws.read(wait, counters).await? {
            ReadEvent::TimedOut | ReadEvent::Ignored => {}
            ReadEvent::Closed(reason) => {
                record_unexpected_close(counters);
                bail!("WebSocket disconnected during stability phase: {reason}");
            }
            ReadEvent::Binary { .. } => counters.unexpected_messages += 1,
        }
    }
}

async fn send_keepalive(
    ws: &mut WsClient,
    config: &Config,
    counters: &mut ConnectionCounters,
    correlation_id: &str,
) -> Result<()> {
    ws.send(
        &RequestHeartbeat {
            template_id: protocol::REQUEST_HEARTBEAT,
            user_msg: vec![correlation_id.to_owned()],
            ssboe: None,
            usecs: None,
        },
        counters,
    )
    .await?;
    counters.application_heartbeats_sent += 1;
    let deadline = Instant::now() + config.request_timeout;

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            counters.timeouts += 1;
            return Ok(());
        }
        match ws.read(remaining, counters).await? {
            ReadEvent::Binary {
                template_id: protocol::RESPONSE_HEARTBEAT,
                payload,
                ..
            } => {
                let response = messages::parse_heartbeat(&payload).inspect_err(|_| {
                    counters.protocol_errors += 1;
                })?;
                counters.application_heartbeats_received += 1;
                if messages::heartbeat_matches(&response, correlation_id) {
                    return Ok(());
                }
                counters.unexpected_messages += 1;
            }
            ReadEvent::TimedOut => {
                counters.timeouts += 1;
                return Ok(());
            }
            ReadEvent::Closed(reason) => {
                record_unexpected_close(counters);
                bail!("WebSocket disconnected during stability heartbeat: {reason}");
            }
            ReadEvent::Binary { .. } => counters.unexpected_messages += 1,
            ReadEvent::Ignored => {}
        }
    }
}

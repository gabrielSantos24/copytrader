use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use prost::Message;

use crate::{
    config::Config,
    models::{ConnectionCounters, RawSample, SampleOutcome, SampleType},
    rithmic::{
        events::FillTracker,
        messages,
        pb::{ExchangeOrderNotification, RequestHeartbeat},
        protocol,
        websocket::{ReadEvent, WsClient, record_unexpected_close},
    },
};

const READ_POLL_INTERVAL: Duration = Duration::from_secs(5);

pub(crate) async fn run(
    ws: &mut WsClient,
    heartbeat_interval: Duration,
    config: &Config,
    samples: &mut Vec<RawSample>,
    counters: &mut ConnectionCounters,
    tracker: &mut FillTracker,
    values: &mut Vec<f64>,
) -> Result<()> {
    println!(
        "Waiting for {} new fills. This optional metric is experimental Fill Event Age.",
        config.fill_samples
    );
    let deadline = config.fill_wait.map(|duration| Instant::now() + duration);
    let keepalive_every = heartbeat_interval.mul_f64(0.8).max(Duration::from_secs(1));
    let mut next_keepalive = Instant::now() + keepalive_every;
    let mut keepalive_id = 0_u64;

    while values.len() < config.fill_samples {
        if Instant::now() >= next_keepalive {
            keepalive_id += 1;
            let correlation_id = format!("fill-keepalive-{keepalive_id}");
            ws.send(
                &RequestHeartbeat {
                    template_id: protocol::REQUEST_HEARTBEAT,
                    user_msg: vec![correlation_id],
                    ssboe: None,
                    usecs: None,
                },
                counters,
            )
            .await?;
            counters.application_heartbeats_sent += 1;
            next_keepalive = Instant::now() + keepalive_every;
        }

        let mut wait = next_keepalive
            .saturating_duration_since(Instant::now())
            .min(READ_POLL_INTERVAL);
        if let Some(value) = deadline {
            wait = wait.min(value.saturating_duration_since(Instant::now()));
        }
        if wait.is_zero() && deadline.is_some_and(|value| Instant::now() >= value) {
            counters.timeouts += 1;
            bail!(
                "fill collection timed out after {} of {} samples",
                values.len(),
                config.fill_samples
            );
        }

        match ws.read(wait, counters).await? {
            ReadEvent::Binary {
                received_at_utc,
                template_id: protocol::EXCHANGE_ORDER_NOTIFICATION,
                payload,
                ..
            } => {
                next_keepalive = Instant::now() + keepalive_every;
                let message = ExchangeOrderNotification::decode(payload.as_slice())
                    .context("invalid ExchangeOrderNotification")?;
                if let Some(fill) = tracker.observe(message, counters) {
                    let Some(microseconds) = received_at_utc
                        .signed_duration_since(fill.source_timestamp)
                        .num_microseconds()
                    else {
                        counters.fills_without_source_timestamp += 1;
                        continue;
                    };
                    let event_age_ms = microseconds as f64 / 1_000.0;
                    let sequence = values.len() + 1;
                    samples.push(RawSample {
                        sequence,
                        observed_at_utc: received_at_utc,
                        location: config.location.clone(),
                        system: config.system_name.clone(),
                        sample_type: SampleType::FillEventAge,
                        outcome: SampleOutcome::Success,
                        latency_ms: Some(event_age_ms),
                        correlation_id: None,
                        template_id: protocol::EXCHANGE_ORDER_NOTIFICATION,
                        fill_id: Some(fill.fill_id),
                        source_account_id: Some(fill.account_id),
                        event_type: Some("fill"),
                        source_timestamp_utc: Some(fill.source_timestamp),
                        source_ssboe: Some(fill.source_ssboe),
                        source_usecs: fill.source_usecs,
                        source_nsecs: fill.source_nsecs,
                        protocol_server_received_ssboe: fill.protocol_server_received_ssboe,
                        protocol_server_received_usecs: fill.protocol_server_received_usecs,
                    });
                    values.push(event_age_ms);
                    println!("  Fill events: {}/{}", values.len(), config.fill_samples);
                }
            }
            ReadEvent::Binary {
                template_id: protocol::RESPONSE_HEARTBEAT,
                payload,
                ..
            } => {
                messages::parse_heartbeat(&payload).inspect_err(|_| {
                    counters.protocol_errors += 1;
                })?;
                counters.application_heartbeats_received += 1;
                next_keepalive = Instant::now() + keepalive_every;
            }
            ReadEvent::TimedOut | ReadEvent::Ignored => {}
            ReadEvent::Closed(reason) => {
                record_unexpected_close(counters);
                bail!("WebSocket disconnected during fill collection: {reason}");
            }
            ReadEvent::Binary { .. } => counters.unexpected_messages += 1,
        }
    }
    Ok(())
}

use std::time::Instant;

use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use tokio::time::sleep;

use crate::{
    config::Config,
    models::{ConnectionCounters, RawSample, SampleOutcome, SampleType},
    rithmic::{
        messages,
        pb::RequestHeartbeat,
        protocol,
        websocket::{ReadEvent, WsClient, record_unexpected_close},
    },
};

struct SuccessfulAttempt {
    received_at_utc: DateTime<Utc>,
    latency_ms: f64,
}

enum AttemptResult {
    Success(SuccessfulAttempt),
    Timeout,
}

pub(crate) async fn warm_up(
    ws: &mut WsClient,
    config: &Config,
    counters: &mut ConnectionCounters,
) -> Result<()> {
    if config.warmup_samples == 0 {
        return Ok(());
    }
    println!(
        "Warming up with {} unrecorded heartbeat round trips",
        config.warmup_samples
    );
    for sequence in 1..=config.warmup_samples {
        let correlation_id = format!("warmup-{sequence}");
        match heartbeat_attempt(ws, config, counters, &correlation_id).await? {
            AttemptResult::Success(_) => {}
            AttemptResult::Timeout => {
                bail!("warm-up heartbeat {sequence} timed out; benchmark not started")
            }
        }
        if sequence < config.warmup_samples {
            sleep(config.rtt_interval).await;
        }
    }
    Ok(())
}

pub(crate) async fn run(
    ws: &mut WsClient,
    config: &Config,
    samples: &mut Vec<RawSample>,
    counters: &mut ConnectionCounters,
    values: &mut Vec<f64>,
) -> Result<()> {
    println!(
        "Collecting {} sequential application-level RTT attempts",
        config.rtt_samples
    );
    for sequence in 1..=config.rtt_samples {
        let correlation_id = format!("rtt-{sequence}");
        match heartbeat_attempt(ws, config, counters, &correlation_id).await? {
            AttemptResult::Success(result) => {
                values.push(result.latency_ms);
                samples.push(RawSample {
                    sequence,
                    observed_at_utc: result.received_at_utc,
                    location: config.location.clone(),
                    system: config.system_name.clone(),
                    sample_type: SampleType::ApiRtt,
                    outcome: SampleOutcome::Success,
                    latency_ms: Some(result.latency_ms),
                    correlation_id: Some(correlation_id),
                    template_id: protocol::RESPONSE_HEARTBEAT,
                    fill_id: None,
                    source_account_id: None,
                    event_type: None,
                    source_timestamp_utc: None,
                    source_ssboe: None,
                    source_usecs: None,
                    source_nsecs: None,
                    protocol_server_received_ssboe: None,
                    protocol_server_received_usecs: None,
                });
            }
            AttemptResult::Timeout => {
                samples.push(RawSample {
                    sequence,
                    observed_at_utc: Utc::now(),
                    location: config.location.clone(),
                    system: config.system_name.clone(),
                    sample_type: SampleType::ApiRtt,
                    outcome: SampleOutcome::Timeout,
                    latency_ms: None,
                    correlation_id: Some(correlation_id),
                    template_id: protocol::RESPONSE_HEARTBEAT,
                    fill_id: None,
                    source_account_id: None,
                    event_type: None,
                    source_timestamp_utc: None,
                    source_ssboe: None,
                    source_usecs: None,
                    source_nsecs: None,
                    protocol_server_received_ssboe: None,
                    protocol_server_received_usecs: None,
                });
            }
        }

        if sequence % 100 == 0 || sequence == config.rtt_samples {
            println!(
                "  RTT attempts: {sequence}/{}; successful: {}",
                config.rtt_samples,
                values.len()
            );
        }
        if sequence < config.rtt_samples {
            sleep(config.rtt_interval).await;
        }
    }
    Ok(())
}

async fn heartbeat_attempt(
    ws: &mut WsClient,
    config: &Config,
    counters: &mut ConnectionCounters,
    correlation_id: &str,
) -> Result<AttemptResult> {
    let request = RequestHeartbeat {
        template_id: protocol::REQUEST_HEARTBEAT,
        user_msg: vec![correlation_id.to_owned()],
        ssboe: None,
        usecs: None,
    };
    let sent_at = ws.send_timed(&request, counters).await?;
    counters.application_heartbeats_sent += 1;
    let deadline = sent_at + config.request_timeout;

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            counters.timeouts += 1;
            return Ok(AttemptResult::Timeout);
        }
        match ws.read(remaining, counters).await? {
            ReadEvent::Binary {
                received_at_instant,
                received_at_utc,
                template_id: protocol::RESPONSE_HEARTBEAT,
                payload,
            } => {
                let response = messages::parse_heartbeat(&payload).inspect_err(|_| {
                    counters.protocol_errors += 1;
                })?;
                counters.application_heartbeats_received += 1;
                if !messages::heartbeat_matches(&response, correlation_id) {
                    counters.unexpected_messages += 1;
                    continue;
                }
                let latency_ms = received_at_instant
                    .saturating_duration_since(sent_at)
                    .as_secs_f64()
                    * 1_000.0;
                return Ok(AttemptResult::Success(SuccessfulAttempt {
                    received_at_utc,
                    latency_ms,
                }));
            }
            ReadEvent::TimedOut => {
                counters.timeouts += 1;
                return Ok(AttemptResult::Timeout);
            }
            ReadEvent::Closed(reason) => {
                record_unexpected_close(counters);
                bail!("WebSocket disconnected during RTT: {reason}");
            }
            ReadEvent::Binary { .. } => counters.unexpected_messages += 1,
            ReadEvent::Ignored => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::rithmic::pb::ResponseHeartbeat;

    use super::*;

    #[test]
    fn heartbeat_correlation_is_exact() {
        let response = ResponseHeartbeat {
            template_id: protocol::RESPONSE_HEARTBEAT,
            user_msg: vec!["rtt-42".to_owned()],
            rp_code: vec!["0".to_owned()],
            ssboe: None,
            usecs: None,
        };
        assert!(messages::heartbeat_matches(&response, "rtt-42"));
        assert!(!messages::heartbeat_matches(&response, "rtt-4"));
    }
}

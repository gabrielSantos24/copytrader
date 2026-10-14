use chrono::{DateTime, Utc};
use serde::Serialize;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SampleType {
    ApiRtt,
    FillEventAge,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SampleOutcome {
    Success,
    Timeout,
}

#[derive(Debug, Serialize)]
pub(crate) struct RawSample {
    pub(crate) sequence: usize,
    pub(crate) observed_at_utc: DateTime<Utc>,
    pub(crate) location: String,
    pub(crate) system: String,
    pub(crate) sample_type: SampleType,
    pub(crate) outcome: SampleOutcome,
    pub(crate) latency_ms: Option<f64>,
    pub(crate) correlation_id: Option<String>,
    pub(crate) template_id: i32,
    pub(crate) fill_id: Option<String>,
    pub(crate) source_account_id: Option<String>,
    pub(crate) event_type: Option<&'static str>,
    pub(crate) source_timestamp_utc: Option<DateTime<Utc>>,
    pub(crate) source_ssboe: Option<i32>,
    pub(crate) source_usecs: Option<i32>,
    pub(crate) source_nsecs: Option<i32>,
    pub(crate) protocol_server_received_ssboe: Option<i32>,
    pub(crate) protocol_server_received_usecs: Option<i32>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct MetricStats {
    pub(crate) samples: usize,
    pub(crate) min_ms: f64,
    pub(crate) mean_ms: f64,
    pub(crate) stddev_ms: f64,
    pub(crate) mean_abs_successive_delta_ms: f64,
    pub(crate) p50_ms: f64,
    pub(crate) p95_ms: f64,
    pub(crate) p99_ms: f64,
    pub(crate) max_ms: f64,
    pub(crate) percentile_method: &'static str,
    pub(crate) stddev_method: &'static str,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct ConnectionCounters {
    pub(crate) connections_opened: u64,
    pub(crate) discovery_connections: u64,
    pub(crate) benchmark_connections: u64,
    pub(crate) connect_failures: u64,
    pub(crate) intentional_closes: u64,
    pub(crate) disconnects: u64,
    pub(crate) unexpected_closes: u64,
    pub(crate) reconnects: u64,
    pub(crate) authentication_failures: u64,
    pub(crate) read_errors: u64,
    pub(crate) write_errors: u64,
    pub(crate) close_errors: u64,
    pub(crate) timeouts: u64,
    pub(crate) protocol_errors: u64,
    pub(crate) unexpected_messages: u64,
    pub(crate) application_heartbeats_sent: u64,
    pub(crate) application_heartbeats_received: u64,
    pub(crate) websocket_pings_received: u64,
    pub(crate) duplicate_fills: u64,
    pub(crate) wrong_account_fills: u64,
    pub(crate) snapshot_fills_ignored: u64,
    pub(crate) fills_without_id: u64,
    pub(crate) fills_without_source_timestamp: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct RunConfiguration {
    pub(crate) websocket_url: &'static str,
    pub(crate) system_name: String,
    pub(crate) source_account_id: Option<String>,
    pub(crate) template_version: &'static str,
    pub(crate) benchmark_request: &'static str,
    pub(crate) one_request_outstanding: bool,
    pub(crate) tcp_nodelay: bool,
    pub(crate) warmup_samples: usize,
    pub(crate) rtt_attempts_requested: usize,
    pub(crate) fill_samples_requested: usize,
    pub(crate) rtt_interval_ms: u64,
    pub(crate) request_timeout_secs: u64,
    pub(crate) connect_timeout_secs: u64,
    pub(crate) stability_secs: u64,
    pub(crate) fill_wait_secs: Option<u64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct HostMetadata {
    pub(crate) hostname: Option<String>,
    pub(crate) os: &'static str,
    pub(crate) architecture: &'static str,
    pub(crate) process_id: u32,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RunStatus {
    Completed,
    Partial,
}

#[derive(Debug, Serialize)]
pub(crate) struct RunSummary {
    pub(crate) methodology_version: &'static str,
    pub(crate) program_version: String,
    pub(crate) location: String,
    pub(crate) environment: &'static str,
    pub(crate) host: HostMetadata,
    pub(crate) run_started_at_utc: DateTime<Utc>,
    pub(crate) run_ended_at_utc: DateTime<Utc>,
    pub(crate) run_duration_ms: f64,
    pub(crate) rtt_started_at_utc: Option<DateTime<Utc>>,
    pub(crate) rtt_ended_at_utc: Option<DateTime<Utc>>,
    pub(crate) status: RunStatus,
    pub(crate) error: Option<String>,
    pub(crate) available_systems: Vec<String>,
    pub(crate) server_heartbeat_interval_secs: Option<f64>,
    pub(crate) configuration: RunConfiguration,
    pub(crate) rtt_attempts: usize,
    pub(crate) rtt_successes: usize,
    pub(crate) rtt_failures: usize,
    pub(crate) rtt_attempts_not_completed: usize,
    pub(crate) fill_samples_collected: usize,
    pub(crate) api_rtt: Option<MetricStats>,
    pub(crate) fill_event_age: Option<MetricStats>,
    pub(crate) fill_metric_status: &'static str,
    pub(crate) connection_policy: &'static str,
    pub(crate) counters: ConnectionCounters,
}

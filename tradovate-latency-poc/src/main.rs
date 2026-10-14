mod benchmark;
mod cli;
mod config;
mod models;
mod output;
mod rithmic;

use std::time::Instant;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;

use benchmark::{fill_event, rtt, stability, statistics::calculate_stats};
use cli::Cli;
use config::{Config, METHODOLOGY_VERSION};
use models::{ConnectionCounters, HostMetadata, RawSample, RunStatus, RunSummary, SampleType};
use rithmic::{events::FillTracker, session};

#[derive(Default)]
struct RunState {
    available_systems: Vec<String>,
    server_heartbeat_interval_secs: Option<f64>,
    rtt_started_at_utc: Option<DateTime<Utc>>,
    rtt_ended_at_utc: Option<DateTime<Utc>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    initialize_tls()?;
    let config = Config::try_from(Cli::parse())?;
    println!("Phase 1/7: configuration");
    let run_started_at = Utc::now();
    let run_started = Instant::now();
    let mut counters = ConnectionCounters::default();
    let mut state = RunState::default();
    let mut samples = Vec::with_capacity(config.rtt_samples + config.fill_samples);
    let mut rtt_values = Vec::with_capacity(config.rtt_samples);
    let mut fill_values = Vec::with_capacity(config.fill_samples);

    let benchmark_result = run_benchmark(
        &config,
        &mut counters,
        &mut state,
        &mut samples,
        &mut rtt_values,
        &mut fill_values,
    )
    .await;

    let rtt_attempts = samples
        .iter()
        .filter(|sample| sample.sample_type == SampleType::ApiRtt)
        .count();
    let summary = RunSummary {
        methodology_version: METHODOLOGY_VERSION,
        program_version: env!("CARGO_PKG_VERSION").to_owned(),
        location: config.location.clone(),
        environment: "rithmic_test",
        host: host_metadata(),
        run_started_at_utc: run_started_at,
        run_ended_at_utc: Utc::now(),
        run_duration_ms: run_started.elapsed().as_secs_f64() * 1_000.0,
        rtt_started_at_utc: state.rtt_started_at_utc,
        rtt_ended_at_utc: state.rtt_ended_at_utc,
        status: if benchmark_result.is_ok() {
            RunStatus::Completed
        } else {
            RunStatus::Partial
        },
        error: benchmark_result.as_ref().err().map(ToString::to_string),
        available_systems: state.available_systems,
        server_heartbeat_interval_secs: state.server_heartbeat_interval_secs,
        configuration: config.summary(),
        rtt_attempts,
        rtt_successes: rtt_values.len(),
        rtt_failures: rtt_attempts.saturating_sub(rtt_values.len()),
        rtt_attempts_not_completed: config.rtt_samples.saturating_sub(rtt_attempts),
        fill_samples_collected: fill_values.len(),
        api_rtt: calculate_stats(&rtt_values),
        fill_event_age: calculate_stats(&fill_values),
        fill_metric_status: "experimental cross-clock event age; one-way fill latency is BLOCKED by timestamp semantics and clock offset",
        connection_policy: "single persistent benchmark connection; abort on disconnect; no automatic reconnect inside a run",
        counters,
    };

    println!("Phase 7/7: export");
    let paths = output::write_results(&config, run_started_at, &samples, &summary)?;
    output::print_summary(&summary, &paths);
    benchmark_result
}

fn initialize_tls() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("failed to install the rustls ring CryptoProvider"))
}

async fn run_benchmark(
    config: &Config,
    counters: &mut ConnectionCounters,
    state: &mut RunState,
    samples: &mut Vec<RawSample>,
    rtt_values: &mut Vec<f64>,
    fill_values: &mut Vec<f64>,
) -> Result<()> {
    println!("Phase 2/7: CONNECT - SystemInfo on discovery WebSocket");
    state.available_systems = session::discover_systems(config, counters)
        .await
        .context("Rithmic system discovery failed")?;

    println!("Phase 3/7: AUTHENTICATE - new Order Plant WebSocket");
    let mut login_session = session::login(config, counters)
        .await
        .context("Rithmic Test login failed")?;
    state.server_heartbeat_interval_secs = Some(login_session.heartbeat_interval.as_secs_f64());

    let phase_result = tokio::select! {
        result = async {
        println!("Phase 4/7: WARM-UP");
        rtt::warm_up(&mut login_session.ws, config, counters).await?;

        println!("Phase 5/7: BENCHMARK");
        state.rtt_started_at_utc = Some(Utc::now());
        let rtt_result =
            rtt::run(&mut login_session.ws, config, samples, counters, rtt_values).await;
        state.rtt_ended_at_utc = Some(Utc::now());
        rtt_result?;

        stability::run(
            &mut login_session.ws,
            login_session.heartbeat_interval,
            config,
            counters,
        )
        .await?;

        if config.fill_samples > 0 {
            session::prepare_fill_subscription(&mut login_session.ws, config, counters).await?;
            let account_id = config
                .source_account_id
                .clone()
                .context("source account missing after validation")?;
            let mut tracker = FillTracker::new(account_id);
            fill_event::run(
                &mut login_session.ws,
                login_session.heartbeat_interval,
                config,
                samples,
                counters,
                &mut tracker,
                fill_values,
            )
            .await?;
        }
            Ok(())
        } => result,
        signal = tokio::signal::ctrl_c() => {
            signal?;
            Err(anyhow::anyhow!("benchmark stopped by user"))
        }
    };

    println!("Phase 6/7: SHUTDOWN");
    let close_result = login_session.ws.close(counters).await;
    match (phase_result, close_result) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

fn host_metadata() -> HostMetadata {
    HostMetadata {
        hostname: std::env::var("COMPUTERNAME")
            .or_else(|_| std::env::var("HOSTNAME"))
            .ok(),
        os: std::env::consts::OS,
        architecture: std::env::consts::ARCH,
        process_id: std::process::id(),
    }
}

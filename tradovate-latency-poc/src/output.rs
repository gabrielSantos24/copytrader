use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::PathBuf,
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};

use crate::{
    config::Config,
    models::{MetricStats, RawSample, RunSummary},
};

pub(crate) struct OutputPaths {
    pub(crate) csv: PathBuf,
    pub(crate) jsonl: PathBuf,
    pub(crate) summary: PathBuf,
}

pub(crate) fn write_results(
    config: &Config,
    started_at: DateTime<Utc>,
    samples: &[RawSample],
    summary: &RunSummary,
) -> Result<OutputPaths> {
    std::fs::create_dir_all(&config.output_dir)
        .with_context(|| format!("failed to create {}", config.output_dir.display()))?;
    let base = format!(
        "{}_{}",
        sanitize(&config.location),
        started_at.format("%Y%m%dT%H%M%SZ")
    );
    let paths = OutputPaths {
        csv: config.output_dir.join(format!("{base}.csv")),
        jsonl: config.output_dir.join(format!("{base}.jsonl")),
        summary: config.output_dir.join(format!("{base}_summary.json")),
    };

    let mut csv = csv::Writer::from_path(&paths.csv).context("failed to create CSV")?;
    let mut jsonl = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&paths.jsonl)
        .map(BufWriter::new)
        .context("failed to create JSONL")?;
    for sample in samples {
        csv.serialize(sample)
            .context("failed to write CSV sample")?;
        serde_json::to_writer(&mut jsonl, sample).context("failed to write JSONL sample")?;
        writeln!(jsonl).context("failed to terminate JSONL sample")?;
    }
    csv.flush().context("failed to flush CSV")?;
    jsonl.flush().context("failed to flush JSONL")?;

    let file = File::create(&paths.summary).context("failed to create summary JSON")?;
    serde_json::to_writer_pretty(BufWriter::new(file), summary)
        .context("failed to write summary JSON")?;
    Ok(paths)
}

pub(crate) fn print_summary(summary: &RunSummary, paths: &OutputPaths) {
    println!();
    println!("Location: {}", summary.location);
    println!("System: {}", summary.configuration.system_name);
    println!(
        "RTT attempts completed: {}/{}",
        summary.rtt_attempts, summary.configuration.rtt_attempts_requested
    );
    println!("RTT successes: {}", summary.rtt_successes);
    println!("RTT completed failures: {}", summary.rtt_failures);
    println!(
        "RTT attempts not completed: {}",
        summary.rtt_attempts_not_completed
    );
    print_metric("Application-level API RTT", &summary.api_rtt);
    if summary.configuration.fill_samples_requested > 0 {
        print_metric(
            "Fill Event Age (experimental; cross-clock)",
            &summary.fill_event_age,
        );
        println!("  Status: {}", summary.fill_metric_status);
    }
    println!();
    println!("Connection");
    println!(
        "  Connections opened: {}",
        summary.counters.connections_opened
    );
    println!("  Reconnects: {}", summary.counters.reconnects);
    println!(
        "  Unexpected closes: {}",
        summary.counters.unexpected_closes
    );
    println!("  Timeouts: {}", summary.counters.timeouts);
    println!("  Read errors: {}", summary.counters.read_errors);
    println!("  Write errors: {}", summary.counters.write_errors);
    println!("  Close errors: {}", summary.counters.close_errors);
    println!("  Protocol errors: {}", summary.counters.protocol_errors);
    println!();
    println!("CSV: {}", paths.csv.display());
    println!("JSONL: {}", paths.jsonl.display());
    println!("Summary JSON: {}", paths.summary.display());
}

fn print_metric(title: &str, stats: &Option<MetricStats>) {
    println!();
    println!("{title}");
    let Some(stats) = stats else {
        println!("  Samples: 0");
        return;
    };
    println!("  Samples: {}", stats.samples);
    println!("  Min: {:.3} ms", stats.min_ms);
    println!("  Mean: {:.3} ms", stats.mean_ms);
    println!("  Stddev: {:.3} ms", stats.stddev_ms);
    println!(
        "  Mean absolute successive delta: {:.3} ms",
        stats.mean_abs_successive_delta_ms
    );
    println!("  P50: {:.3} ms", stats.p50_ms);
    println!("  P95: {:.3} ms", stats.p95_ms);
    println!("  P99: {:.3} ms", stats.p99_ms);
    println!("  Max: {:.3} ms", stats.max_ms);
}

fn sanitize(value: &str) -> String {
    let result = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    result.trim_matches('-').to_owned()
}

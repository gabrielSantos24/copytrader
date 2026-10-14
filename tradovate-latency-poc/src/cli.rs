use std::path::PathBuf;

use clap::Parser;

#[derive(Parser)]
#[command(
    author,
    version,
    about = "Controlled Rithmic Test R|Protocol latency benchmark"
)]
pub(crate) struct Cli {
    #[arg(long, env = "RITHMIC_LOCATION")]
    pub(crate) location: String,
    #[arg(long, env = "RITHMIC_SOURCE_ACCOUNT_ID")]
    pub(crate) source_account_id: Option<String>,
    #[arg(long, env = "RITHMIC_SYSTEM_NAME", default_value = "Rithmic Test")]
    pub(crate) system_name: String,
    #[arg(long, env = "RITHMIC_APP_NAME", default_value = "rithmic-latency-poc")]
    pub(crate) app_name: String,
    #[arg(long, env = "RITHMIC_APP_VERSION", default_value = env!("CARGO_PKG_VERSION"))]
    pub(crate) app_version: String,
    #[arg(long, env = "RITHMIC_WARMUP_SAMPLES", default_value_t = 50)]
    pub(crate) warmup_samples: usize,
    #[arg(long, env = "RITHMIC_RTT_SAMPLES", default_value_t = 1_000)]
    pub(crate) rtt_samples: usize,
    #[arg(long, env = "RITHMIC_FILL_SAMPLES", default_value_t = 0)]
    pub(crate) fill_samples: usize,
    #[arg(long, env = "RITHMIC_RTT_INTERVAL_MS", default_value_t = 250)]
    pub(crate) rtt_interval_ms: u64,
    #[arg(long, env = "RITHMIC_REQUEST_TIMEOUT_SECS", default_value_t = 5)]
    pub(crate) request_timeout_secs: u64,
    #[arg(long, env = "RITHMIC_CONNECT_TIMEOUT_SECS", default_value_t = 30)]
    pub(crate) connect_timeout_secs: u64,
    #[arg(long, env = "RITHMIC_STABILITY_SECS", default_value_t = 0)]
    pub(crate) stability_secs: u64,
    #[arg(long, env = "RITHMIC_FILL_WAIT_SECS", default_value_t = 0)]
    pub(crate) fill_wait_secs: u64,
    #[arg(long, env = "RITHMIC_OUTPUT_DIR", default_value = "results")]
    pub(crate) output_dir: PathBuf,
}

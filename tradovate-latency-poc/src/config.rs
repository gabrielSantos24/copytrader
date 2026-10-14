use std::{env, path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail};

use crate::{cli::Cli, models::RunConfiguration};

pub(crate) const WS_URL: &str = "wss://rituz00100.rithmic.com:443";
pub(crate) const TEMPLATE_VERSION: &str = "5.55";
pub(crate) const METHODOLOGY_VERSION: &str = "rithmic-heartbeat-rtt-v2";

pub(crate) struct Config {
    pub(crate) location: String,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) source_account_id: Option<String>,
    pub(crate) system_name: String,
    pub(crate) app_name: String,
    pub(crate) app_version: String,
    pub(crate) warmup_samples: usize,
    pub(crate) rtt_samples: usize,
    pub(crate) fill_samples: usize,
    pub(crate) rtt_interval: Duration,
    pub(crate) request_timeout: Duration,
    pub(crate) connect_timeout: Duration,
    pub(crate) stability_duration: Duration,
    pub(crate) fill_wait: Option<Duration>,
    pub(crate) output_dir: PathBuf,
}

impl TryFrom<Cli> for Config {
    type Error = anyhow::Error;

    fn try_from(cli: Cli) -> Result<Self> {
        let username = env::var("RITHMIC_USERNAME").context("RITHMIC_USERNAME is required")?;
        let password = env::var("RITHMIC_PASSWORD").context("RITHMIC_PASSWORD is required")?;
        Self::from_cli_with_credentials(cli, username, password)
    }
}

impl Config {
    fn from_cli_with_credentials(cli: Cli, username: String, password: String) -> Result<Self> {
        validate_non_empty("location", &cli.location)?;
        validate_non_empty("RITHMIC_USERNAME", &username)?;
        validate_non_empty("RITHMIC_PASSWORD", &password)?;
        validate_non_empty("RITHMIC_SYSTEM_NAME", &cli.system_name)?;
        validate_non_empty("RITHMIC_APP_NAME", &cli.app_name)?;
        validate_non_empty("RITHMIC_APP_VERSION", &cli.app_version)?;

        let source_account_id = cli
            .source_account_id
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty());
        if cli.fill_samples > 0 && source_account_id.is_none() {
            bail!("RITHMIC_SOURCE_ACCOUNT_ID is required when fill_samples is greater than zero");
        }
        if cli.rtt_samples == 0 {
            bail!("rtt_samples must be greater than zero");
        }
        if cli.rtt_interval_ms == 0
            || cli.request_timeout_secs == 0
            || cli.connect_timeout_secs == 0
        {
            bail!("RTT interval, request timeout and connect timeout must be greater than zero");
        }
        if cli.output_dir.as_os_str().is_empty() {
            bail!("output_dir must not be empty");
        }
        let location = cli.location.trim().to_owned();
        if !location.chars().any(char::is_alphanumeric) {
            bail!("location must contain at least one alphanumeric character");
        }

        Ok(Self {
            location,
            username,
            password,
            source_account_id,
            system_name: cli.system_name.trim().to_owned(),
            app_name: cli.app_name.trim().to_owned(),
            app_version: cli.app_version.trim().to_owned(),
            warmup_samples: cli.warmup_samples,
            rtt_samples: cli.rtt_samples,
            fill_samples: cli.fill_samples,
            rtt_interval: Duration::from_millis(cli.rtt_interval_ms),
            request_timeout: Duration::from_secs(cli.request_timeout_secs),
            connect_timeout: Duration::from_secs(cli.connect_timeout_secs),
            stability_duration: Duration::from_secs(cli.stability_secs),
            fill_wait: (cli.fill_wait_secs > 0).then(|| Duration::from_secs(cli.fill_wait_secs)),
            output_dir: cli.output_dir,
        })
    }

    pub(crate) fn summary(&self) -> RunConfiguration {
        RunConfiguration {
            websocket_url: WS_URL,
            system_name: self.system_name.clone(),
            source_account_id: self.source_account_id.clone(),
            template_version: TEMPLATE_VERSION,
            benchmark_request: "RequestHeartbeat (18) -> ResponseHeartbeat (19)",
            one_request_outstanding: true,
            tcp_nodelay: true,
            warmup_samples: self.warmup_samples,
            rtt_attempts_requested: self.rtt_samples,
            fill_samples_requested: self.fill_samples,
            rtt_interval_ms: self.rtt_interval.as_millis() as u64,
            request_timeout_secs: self.request_timeout.as_secs(),
            connect_timeout_secs: self.connect_timeout.as_secs(),
            stability_secs: self.stability_duration.as_secs(),
            fill_wait_secs: self.fill_wait.map(|value| value.as_secs()),
        }
    }
}

fn validate_non_empty(name: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{name} must not be empty");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cli() -> Cli {
        Cli {
            location: "chicago".to_owned(),
            source_account_id: None,
            system_name: "Rithmic Test".to_owned(),
            app_name: "poc".to_owned(),
            app_version: "1".to_owned(),
            warmup_samples: 10,
            rtt_samples: 1000,
            fill_samples: 0,
            rtt_interval_ms: 250,
            request_timeout_secs: 5,
            connect_timeout_secs: 30,
            stability_secs: 0,
            fill_wait_secs: 0,
            output_dir: PathBuf::from("results"),
        }
    }

    #[test]
    fn accepts_rtt_only_without_an_account() {
        let config =
            Config::from_cli_with_credentials(cli(), "user".to_owned(), "password".to_owned())
                .unwrap();
        assert!(config.source_account_id.is_none());
        assert_eq!(config.rtt_samples, 1000);
    }

    #[test]
    fn fill_collection_requires_an_account() {
        let mut value = cli();
        value.fill_samples = 1;
        let error =
            Config::from_cli_with_credentials(value, "user".to_owned(), "password".to_owned())
                .err()
                .unwrap();
        assert!(error.to_string().contains("SOURCE_ACCOUNT_ID"));
    }

    #[test]
    fn rejects_zero_rtt_attempts_and_blank_location() {
        let mut value = cli();
        value.rtt_samples = 0;
        assert!(
            Config::from_cli_with_credentials(value, "user".to_owned(), "password".to_owned())
                .is_err()
        );

        let mut value = cli();
        value.location = "---".to_owned();
        assert!(
            Config::from_cli_with_credentials(value, "user".to_owned(), "password".to_owned())
                .is_err()
        );
    }
}

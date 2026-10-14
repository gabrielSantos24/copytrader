use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use prost::Message;

use crate::{
    config::{Config, TEMPLATE_VERSION},
    models::ConnectionCounters,
    rithmic::{
        messages,
        pb::{
            RequestAccountList, RequestLogin, RequestLoginInfo, RequestRithmicSystemInfo,
            RequestSubscribeForOrderUpdates, ResponseAccountList, ResponseLoginInfo,
            ResponseSubscribeForOrderUpdates,
        },
        protocol,
        websocket::{ConnectionRole, ReadEvent, WsClient, record_unexpected_close},
    },
};

const ORDER_PLANT: i32 = 2;

pub(crate) struct LoginSession {
    pub(crate) ws: WsClient,
    pub(crate) heartbeat_interval: Duration,
}

pub(crate) async fn discover_systems(
    config: &Config,
    counters: &mut ConnectionCounters,
) -> Result<Vec<String>> {
    let mut ws =
        WsClient::connect(config.connect_timeout, ConnectionRole::Discovery, counters).await?;
    ws.send(
        &RequestRithmicSystemInfo {
            template_id: protocol::REQUEST_SYSTEM_INFO,
            user_msg: vec!["system-info".to_owned()],
        },
        counters,
    )
    .await?;
    let payload = wait_for_template(
        &mut ws,
        protocol::RESPONSE_SYSTEM_INFO,
        config.request_timeout,
        counters,
        "system info",
    )
    .await?;
    let response = messages::parse_system_info(&payload)?;
    if !response
        .system_name
        .iter()
        .any(|name| name == &config.system_name)
    {
        bail!(
            "configured system '{}' is unavailable; server returned: {}",
            config.system_name,
            response.system_name.join(", ")
        );
    }
    ws.close(counters).await?;
    Ok(response.system_name)
}

pub(crate) async fn login(
    config: &Config,
    counters: &mut ConnectionCounters,
) -> Result<LoginSession> {
    let mut ws =
        WsClient::connect(config.connect_timeout, ConnectionRole::Benchmark, counters).await?;
    ws.send(
        &RequestLogin {
            template_id: protocol::REQUEST_LOGIN,
            template_version: Some(TEMPLATE_VERSION.to_owned()),
            user_msg: vec!["login".to_owned()],
            user: Some(config.username.clone()),
            password: Some(config.password.clone()),
            app_name: Some(config.app_name.clone()),
            app_version: Some(config.app_version.clone()),
            system_name: Some(config.system_name.clone()),
            infra_type: Some(ORDER_PLANT),
            mac_addr: Vec::new(),
            os_version: Some(std::env::consts::OS.to_owned()),
            os_platform: Some(std::env::consts::ARCH.to_owned()),
            aggregated_quotes: None,
        },
        counters,
    )
    .await?;
    let payload = wait_for_template(
        &mut ws,
        protocol::RESPONSE_LOGIN,
        config.request_timeout,
        counters,
        "login",
    )
    .await?;
    let response = match messages::parse_login(&payload) {
        Ok(value) => value,
        Err(error) => {
            counters.authentication_failures += 1;
            return Err(error);
        }
    };
    let heartbeat_secs = response
        .heartbeat_interval
        .filter(|value| value.is_finite() && *value > 0.0)
        .context("successful login did not include a valid heartbeat_interval")?;

    Ok(LoginSession {
        ws,
        heartbeat_interval: Duration::from_secs_f64(heartbeat_secs),
    })
}

pub(crate) async fn prepare_fill_subscription(
    ws: &mut WsClient,
    config: &Config,
    counters: &mut ConnectionCounters,
) -> Result<()> {
    let account_id = config
        .source_account_id
        .as_deref()
        .context("source account is required for fill collection")?;
    let (fcm_id, ib_id, user_type) = login_info(ws, config, counters).await?;
    validate_account(ws, config, counters, account_id, &fcm_id, &ib_id, user_type).await?;
    subscribe(ws, config, counters, account_id, &fcm_id, &ib_id).await
}

async fn login_info(
    ws: &mut WsClient,
    config: &Config,
    counters: &mut ConnectionCounters,
) -> Result<(String, String, i32)> {
    ws.send(
        &RequestLoginInfo {
            template_id: protocol::REQUEST_LOGIN_INFO,
            user_msg: vec!["login-info".to_owned()],
        },
        counters,
    )
    .await?;
    let payload = wait_for_template(
        ws,
        protocol::RESPONSE_LOGIN_INFO,
        config.request_timeout,
        counters,
        "login info",
    )
    .await?;
    let response =
        ResponseLoginInfo::decode(payload.as_slice()).context("invalid ResponseLoginInfo")?;
    protocol::ensure_success("login info", &response.rp_code)?;
    Ok((
        response
            .fcm_id
            .context("login info did not include fcm_id")?,
        response.ib_id.context("login info did not include ib_id")?,
        response
            .user_type
            .context("login info did not include user_type")?,
    ))
}

async fn validate_account(
    ws: &mut WsClient,
    config: &Config,
    counters: &mut ConnectionCounters,
    account_id: &str,
    fcm_id: &str,
    ib_id: &str,
    user_type: i32,
) -> Result<()> {
    ws.send(
        &RequestAccountList {
            template_id: protocol::REQUEST_ACCOUNT_LIST,
            user_msg: vec!["account-list".to_owned()],
            fcm_id: Some(fcm_id.to_owned()),
            ib_id: Some(ib_id.to_owned()),
            user_type: Some(user_type),
        },
        counters,
    )
    .await?;

    let deadline = Instant::now() + config.request_timeout;
    let mut found = false;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            counters.timeouts += 1;
            bail!("account list timed out");
        }
        match ws.read(remaining, counters).await? {
            ReadEvent::Binary {
                template_id: protocol::RESPONSE_ACCOUNT_LIST,
                payload,
                ..
            } => {
                let response = ResponseAccountList::decode(payload.as_slice())
                    .context("invalid ResponseAccountList")?;
                if !response.rq_handler_rp_code.is_empty() {
                    protocol::ensure_success("account list item", &response.rq_handler_rp_code)?;
                    found |= response.account_id.as_deref() == Some(account_id);
                }
                if !response.rp_code.is_empty() {
                    protocol::ensure_success("account list", &response.rp_code)?;
                    break;
                }
            }
            ReadEvent::TimedOut => {
                counters.timeouts += 1;
                bail!("account list timed out");
            }
            ReadEvent::Closed(reason) => {
                record_unexpected_close(counters);
                bail!("WebSocket disconnected during account discovery: {reason}");
            }
            ReadEvent::Binary { .. } => counters.unexpected_messages += 1,
            ReadEvent::Ignored => {}
        }
    }
    if !found {
        bail!("source account '{account_id}' was not returned by Rithmic Test");
    }
    Ok(())
}

async fn subscribe(
    ws: &mut WsClient,
    config: &Config,
    counters: &mut ConnectionCounters,
    account_id: &str,
    fcm_id: &str,
    ib_id: &str,
) -> Result<()> {
    ws.send(
        &RequestSubscribeForOrderUpdates {
            template_id: protocol::REQUEST_SUBSCRIBE_ORDER_UPDATES,
            user_msg: vec!["subscribe-order-updates".to_owned()],
            fcm_id: Some(fcm_id.to_owned()),
            ib_id: Some(ib_id.to_owned()),
            account_id: Some(account_id.to_owned()),
        },
        counters,
    )
    .await?;
    let payload = wait_for_template(
        ws,
        protocol::RESPONSE_SUBSCRIBE_ORDER_UPDATES,
        config.request_timeout,
        counters,
        "order update subscription",
    )
    .await?;
    let response = ResponseSubscribeForOrderUpdates::decode(payload.as_slice())
        .context("invalid ResponseSubscribeForOrderUpdates")?;
    protocol::ensure_success("order update subscription", &response.rp_code)
}

async fn wait_for_template(
    ws: &mut WsClient,
    expected: i32,
    timeout: Duration,
    counters: &mut ConnectionCounters,
    operation: &str,
) -> Result<Vec<u8>> {
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            counters.timeouts += 1;
            bail!("{operation} timed out");
        }
        match ws.read(remaining, counters).await? {
            ReadEvent::Binary {
                template_id,
                payload,
                ..
            } if template_id == expected => return Ok(payload),
            ReadEvent::TimedOut => {
                counters.timeouts += 1;
                bail!("{operation} timed out");
            }
            ReadEvent::Closed(reason) => {
                record_unexpected_close(counters);
                bail!("WebSocket disconnected during {operation}: {reason}");
            }
            ReadEvent::Binary { .. } => counters.unexpected_messages += 1,
            ReadEvent::Ignored => {}
        }
    }
}

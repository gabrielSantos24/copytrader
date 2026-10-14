use anyhow::{Context, Result, bail};
use prost::Message;

use crate::rithmic::{
    pb::{ResponseHeartbeat, ResponseLogin, ResponseRithmicSystemInfo},
    protocol,
};

pub(crate) fn parse_system_info(payload: &[u8]) -> Result<ResponseRithmicSystemInfo> {
    let response = ResponseRithmicSystemInfo::decode(payload)
        .context("invalid ResponseRithmicSystemInfo protobuf")?;
    ensure_template(
        "ResponseRithmicSystemInfo",
        response.template_id,
        protocol::RESPONSE_SYSTEM_INFO,
    )?;
    protocol::ensure_success("system info", &response.rp_code)?;
    Ok(response)
}

pub(crate) fn parse_login(payload: &[u8]) -> Result<ResponseLogin> {
    let response = ResponseLogin::decode(payload).context("invalid ResponseLogin protobuf")?;
    ensure_template(
        "ResponseLogin",
        response.template_id,
        protocol::RESPONSE_LOGIN,
    )?;
    protocol::ensure_success("login", &response.rp_code)?;
    Ok(response)
}

pub(crate) fn parse_heartbeat(payload: &[u8]) -> Result<ResponseHeartbeat> {
    let response =
        ResponseHeartbeat::decode(payload).context("invalid ResponseHeartbeat protobuf")?;
    ensure_template(
        "ResponseHeartbeat",
        response.template_id,
        protocol::RESPONSE_HEARTBEAT,
    )?;
    protocol::ensure_success("heartbeat", &response.rp_code)?;
    Ok(response)
}

pub(crate) fn heartbeat_matches(response: &ResponseHeartbeat, correlation_id: &str) -> bool {
    response
        .user_msg
        .iter()
        .any(|value| value == correlation_id)
}

fn ensure_template(message: &str, actual: i32, expected: i32) -> Result<()> {
    if actual != expected {
        bail!("{message} had template {actual}, expected {expected}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_system_info_and_available_systems() {
        let payload = ResponseRithmicSystemInfo {
            template_id: protocol::RESPONSE_SYSTEM_INFO,
            user_msg: vec!["system-info".to_owned()],
            rp_code: vec!["0".to_owned()],
            system_name: vec!["Rithmic Test".to_owned(), "Other".to_owned()],
            has_aggregated_quotes: vec![false, true],
        }
        .encode_to_vec();
        let parsed = parse_system_info(&payload).unwrap();
        assert_eq!(parsed.system_name[0], "Rithmic Test");
    }

    #[test]
    fn parses_successful_login_and_rejects_failed_login() {
        let success = ResponseLogin {
            template_id: protocol::RESPONSE_LOGIN,
            user_msg: vec!["login".to_owned()],
            rp_code: vec!["0".to_owned()],
            heartbeat_interval: Some(10.0),
            ..Default::default()
        }
        .encode_to_vec();
        assert_eq!(
            parse_login(&success).unwrap().heartbeat_interval,
            Some(10.0)
        );

        let failure = ResponseLogin {
            template_id: protocol::RESPONSE_LOGIN,
            rp_code: vec!["1".to_owned(), "denied".to_owned()],
            ..Default::default()
        }
        .encode_to_vec();
        assert!(parse_login(&failure).is_err());
    }

    #[test]
    fn heartbeat_requires_matching_echo_and_valid_protobuf() {
        let payload = ResponseHeartbeat {
            template_id: protocol::RESPONSE_HEARTBEAT,
            user_msg: vec!["rtt-7".to_owned()],
            rp_code: vec!["0".to_owned()],
            ssboe: None,
            usecs: None,
        }
        .encode_to_vec();
        let response = parse_heartbeat(&payload).unwrap();
        assert!(heartbeat_matches(&response, "rtt-7"));
        assert!(!heartbeat_matches(&response, "rtt-8"));
        assert!(parse_heartbeat(&[0xff]).is_err());
    }
}

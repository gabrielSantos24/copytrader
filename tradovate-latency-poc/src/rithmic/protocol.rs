use anyhow::{Result, bail};
use prost::Message;

use crate::rithmic::pb::MessageType;

pub(crate) const REQUEST_LOGIN: i32 = 10;
pub(crate) const RESPONSE_LOGIN: i32 = 11;
pub(crate) const REQUEST_SYSTEM_INFO: i32 = 16;
pub(crate) const RESPONSE_SYSTEM_INFO: i32 = 17;
pub(crate) const REQUEST_HEARTBEAT: i32 = 18;
pub(crate) const RESPONSE_HEARTBEAT: i32 = 19;
pub(crate) const REQUEST_LOGIN_INFO: i32 = 300;
pub(crate) const RESPONSE_LOGIN_INFO: i32 = 301;
pub(crate) const REQUEST_ACCOUNT_LIST: i32 = 302;
pub(crate) const RESPONSE_ACCOUNT_LIST: i32 = 303;
pub(crate) const REQUEST_SUBSCRIBE_ORDER_UPDATES: i32 = 308;
pub(crate) const RESPONSE_SUBSCRIBE_ORDER_UPDATES: i32 = 309;
pub(crate) const EXCHANGE_ORDER_NOTIFICATION: i32 = 352;

pub(crate) fn template_id(bytes: &[u8]) -> Result<i32> {
    Ok(MessageType::decode(bytes)?.template_id)
}

pub(crate) fn ensure_success(operation: &str, rp_code: &[String]) -> Result<()> {
    match rp_code {
        [code] if code == "0" => Ok(()),
        [] => bail!("{operation} response did not contain rp_code"),
        values => bail!("{operation} failed: {}", values.join(" | ")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_template_id_without_decoding_the_full_message() {
        let message = MessageType { template_id: 19 };
        assert_eq!(template_id(&message.encode_to_vec()).unwrap(), 19);
    }

    #[test]
    fn rejects_malformed_protobuf() {
        assert!(template_id(&[0xff, 0xff]).is_err());
    }

    #[test]
    fn requires_the_documented_single_zero_success_code() {
        assert!(ensure_success("test", &["0".to_owned()]).is_ok());
        assert!(ensure_success("test", &[]).is_err());
        assert!(ensure_success("test", &["1".to_owned(), "denied".to_owned()]).is_err());
    }
}

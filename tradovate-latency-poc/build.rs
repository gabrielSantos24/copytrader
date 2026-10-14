use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    unsafe { std::env::set_var("PROTOC", protoc) };

    let proto_dir = std::env::var_os("RITHMIC_PROTO_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("docs/rithmic/sdk/0.90.0.0/proto"));
    if !proto_dir.is_dir() {
        return Err(format!(
            "Rithmic schemas not found at '{}'. Extract the authorized R|Protocol SDK there or set RITHMIC_PROTO_DIR.",
            proto_dir.display()
        )
        .into());
    }

    let names = [
        "message_type.proto",
        "request_rithmic_system_info.proto",
        "response_rithmic_system_info.proto",
        "request_login.proto",
        "response_login.proto",
        "request_login_info.proto",
        "response_login_info.proto",
        "request_heartbeat.proto",
        "response_heartbeat.proto",
        "request_account_list.proto",
        "response_account_list.proto",
        "request_subscribe_for_order_updates.proto",
        "response_subscribe_for_order_updates.proto",
        "exchange_order_notification.proto",
    ];
    let protos = names
        .iter()
        .map(|name| proto_dir.join(name))
        .collect::<Vec<_>>();

    println!("cargo:rerun-if-env-changed=RITHMIC_PROTO_DIR");
    for proto in &protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }
    prost_build::compile_protos(&protos, &[proto_dir])?;
    Ok(())
}

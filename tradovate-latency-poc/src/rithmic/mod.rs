pub(crate) mod events;
pub(crate) mod messages;
pub(crate) mod protocol;
pub(crate) mod session;
pub(crate) mod websocket;

#[allow(clippy::enum_variant_names, clippy::tabs_in_doc_comments)]
pub(crate) mod pb {
    include!(concat!(env!("OUT_DIR"), "/rti.rs"));
}

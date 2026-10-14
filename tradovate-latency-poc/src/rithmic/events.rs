use std::collections::HashSet;

use chrono::{DateTime, Utc};

use crate::{models::ConnectionCounters, rithmic::pb::ExchangeOrderNotification};

const FILL_NOTIFY_TYPE: i32 = 5;

#[derive(Debug)]
pub(crate) struct FillEvent {
    pub(crate) fill_id: String,
    pub(crate) account_id: String,
    pub(crate) source_timestamp: DateTime<Utc>,
    pub(crate) source_ssboe: i32,
    pub(crate) source_usecs: Option<i32>,
    pub(crate) source_nsecs: Option<i32>,
    pub(crate) protocol_server_received_ssboe: Option<i32>,
    pub(crate) protocol_server_received_usecs: Option<i32>,
}

pub(crate) struct FillTracker {
    source_account_id: String,
    seen_fill_ids: HashSet<String>,
}

impl FillTracker {
    pub(crate) fn new(source_account_id: String) -> Self {
        Self {
            source_account_id,
            seen_fill_ids: HashSet::new(),
        }
    }

    pub(crate) fn observe(
        &mut self,
        message: ExchangeOrderNotification,
        counters: &mut ConnectionCounters,
    ) -> Option<FillEvent> {
        if message.notify_type != Some(FILL_NOTIFY_TYPE) {
            return None;
        }
        let Some(fill_id) = message.fill_id.filter(|value| !value.is_empty()) else {
            counters.fills_without_id += 1;
            return None;
        };
        if !self.seen_fill_ids.insert(fill_id.clone()) {
            counters.duplicate_fills += 1;
            return None;
        }
        if message.is_snapshot == Some(true) {
            counters.snapshot_fills_ignored += 1;
            return None;
        }
        let account_id = message.account_id.unwrap_or_default();
        if account_id != self.source_account_id {
            counters.wrong_account_fills += 1;
            return None;
        }

        // These fields are preserved verbatim. Their exact source semantics are not
        // defined by the supplied guide, so the derived value is only Event Age.
        let Some(source_ssboe) = message.source_ssboe else {
            counters.fills_without_source_timestamp += 1;
            return None;
        };
        let nanoseconds = message.source_nsecs.or_else(|| {
            message
                .source_usecs
                .map(|value| value.saturating_mul(1_000))
        });
        let Some(nanoseconds) = nanoseconds.filter(|value| (0..1_000_000_000).contains(value))
        else {
            counters.fills_without_source_timestamp += 1;
            return None;
        };
        let Some(source_timestamp) =
            DateTime::<Utc>::from_timestamp(i64::from(source_ssboe), nanoseconds as u32)
        else {
            counters.fills_without_source_timestamp += 1;
            return None;
        };

        Some(FillEvent {
            fill_id,
            account_id,
            source_timestamp,
            source_ssboe,
            source_usecs: message.source_usecs,
            source_nsecs: message.source_nsecs,
            protocol_server_received_ssboe: message.proto_srvr_rcvd_ssboe,
            protocol_server_received_usecs: message.proto_srvr_rcvd_usecs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fill(id: &str, account: &str) -> ExchangeOrderNotification {
        ExchangeOrderNotification {
            template_id: 352,
            notify_type: Some(FILL_NOTIFY_TYPE),
            fill_id: Some(id.to_owned()),
            account_id: Some(account.to_owned()),
            source_ssboe: Some(1_700_000_000),
            source_nsecs: Some(123_000_000),
            ..Default::default()
        }
    }

    #[test]
    fn identifies_deduplicates_and_filters_fill_events() {
        let mut tracker = FillTracker::new("A1".to_owned());
        let mut counters = ConnectionCounters::default();
        assert!(tracker.observe(fill("F1", "A1"), &mut counters).is_some());
        assert!(tracker.observe(fill("F1", "A1"), &mut counters).is_none());
        assert!(tracker.observe(fill("F2", "A2"), &mut counters).is_none());
        assert_eq!(counters.duplicate_fills, 1);
        assert_eq!(counters.wrong_account_fills, 1);
    }

    #[test]
    fn rejects_snapshot_and_fill_without_source_timestamp() {
        let mut snapshot = fill("F1", "A1");
        snapshot.is_snapshot = Some(true);
        let mut tracker = FillTracker::new("A1".to_owned());
        let mut counters = ConnectionCounters::default();
        assert!(tracker.observe(snapshot, &mut counters).is_none());

        let mut missing = fill("F2", "A1");
        missing.source_nsecs = None;
        missing.source_usecs = None;
        assert!(tracker.observe(missing, &mut counters).is_none());
        assert_eq!(counters.snapshot_fills_ignored, 1);
        assert_eq!(counters.fills_without_source_timestamp, 1);
    }
}

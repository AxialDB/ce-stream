//! In-flight MySQL transaction buffer (commit-boundary emit).

use ce_stream_core::transaction::{CommittedTransaction, DdlStatement};
use ce_stream_core::CloudEvent;

/// Buffers DDL + row CloudEvents until XID.
#[derive(Debug, Default)]
pub struct TxnBuffer {
    pending_gtid: Option<String>,
    ddl: Vec<DdlStatement>,
    events: Vec<CloudEvent>,
}

impl TxnBuffer {
    pub fn pending_gtid(&self) -> Option<&str> {
        self.pending_gtid.as_deref()
    }

    pub fn on_gtid(&mut self, gtid: String) {
        self.pending_gtid = Some(gtid);
        self.ddl.clear();
        self.events.clear();
    }

    pub fn push_ddl(&mut self, stmt: DdlStatement) {
        self.ddl.push(stmt);
    }

    pub fn push_row(&mut self, event: CloudEvent) {
        self.events.push(event);
    }

    /// Build commit payload at XID; clears in-flight buffers but does not clear pending until taken.
    pub fn take_commit(&mut self, gtid_set_after: String) -> Option<CommittedTransaction> {
        let gtid = self.pending_gtid.take()?;
        Some(CommittedTransaction {
            gtid,
            gtid_set_after,
            ddl: std::mem::take(&mut self.ddl),
            events: std::mem::take(&mut self.events),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ce_stream_core::event::{ChangeOp, TableRef};

    fn sample_row() -> CloudEvent {
        CloudEvent::row_change(
            "mysql://test",
            &TableRef::new("db", "t"),
            ChangeOp::Insert,
            serde_json::json!({"op": "insert"}),
            serde_json::Map::new(),
        )
    }

    #[test]
    fn buffers_until_take_commit() {
        let mut buf = TxnBuffer::default();
        buf.on_gtid("uuid:1".into());
        buf.push_row(sample_row());
        buf.push_row(sample_row());
        assert_eq!(buf.events.len(), 2);

        let txn = buf.take_commit("uuid:1-1".into()).expect("commit");
        assert_eq!(txn.gtid, "uuid:1");
        assert_eq!(txn.events.len(), 2);
        assert!(buf.pending_gtid().is_none());
        assert!(buf.events.is_empty());
    }
}

//! In-flight MySQL transaction buffer (commit-boundary emit).

use ce_stream_core::error::Result;
use ce_stream_core::include::IncludeFilter;
use ce_stream_core::source::{DEFAULT_PART_MAX_BYTES, DEFAULT_PART_MAX_ROWS};
use ce_stream_core::transaction::{CommittedTransaction, DdlStatement, TransactionPart};
use ce_stream_core::CloudEvent;

use crate::gtid::ExecutedSet;
use crate::queue::{delivery_bytes, QueueBytes};
use std::sync::Arc;

/// Buffers DDL + row CloudEvents until the transaction ends, or until they
/// fill a part of a long transaction ([`TransactionPart`]).
#[derive(Debug, Default)]
pub struct TxnBuffer {
    pending_gtid: Option<String>,
    /// A `BEGIN` was read: the transaction ends at its `Xid` or `COMMIT`.
    /// Without one, a statement is a transaction by itself.
    open: bool,
    ddl: Vec<DdlStatement>,
    events: Vec<CloudEvent>,
    /// Include snapshot taken at GTID for this envelope.
    include_filter: IncludeFilter,
    /// The set the reader started from and every transaction it has passed
    /// on since. The consumer's executed set is behind it by the commits
    /// that wait in the queue.
    passed_on: Option<ExecutedSet>,
    /// The size of a part; 0 is the default.
    part_max_rows: usize,
    part_max_bytes: usize,
    /// What the rows buffered for the part being filled hold.
    part_rows: usize,
    part_bytes: usize,
    /// Parts of the pending transaction delivered so far.
    parts_sent: u32,
    /// The bytes waiting in the queue to the consumer, when the reader bounds
    /// them (`with_queue`). `None` in tests that read the queue themselves.
    queue: Option<Arc<QueueBytes>>,
}

impl TxnBuffer {
    /// A buffer that cuts a long transaction into parts of at most
    /// `max_rows` rows and `max_bytes` bytes of row data. 0 is the default.
    pub fn with_part_limits(max_rows: usize, max_bytes: usize) -> Self {
        Self {
            part_max_rows: max_rows,
            part_max_bytes: max_bytes,
            ..Self::default()
        }
    }

    fn max_rows(&self) -> usize {
        if self.part_max_rows == 0 {
            DEFAULT_PART_MAX_ROWS
        } else {
            self.part_max_rows
        }
    }

    fn max_bytes(&self) -> usize {
        if self.part_max_bytes == 0 {
            DEFAULT_PART_MAX_BYTES
        } else {
            self.part_max_bytes
        }
    }

    /// Parts of the pending transaction already handed out.
    pub fn parts_sent(&self) -> u32 {
        self.parts_sent
    }

    pub fn pending_gtid(&self) -> Option<&str> {
        self.pending_gtid.as_deref()
    }

    /// Pin the include list for this transaction. Call at GTID, before rows.
    pub(crate) fn set_include(&mut self, filter: IncludeFilter) {
        self.include_filter = filter;
    }

    pub fn allows(&self, subject: &str) -> bool {
        self.include_filter.row_allowed(subject)
    }

    pub fn on_gtid(&mut self, gtid: String) {
        self.pending_gtid = Some(gtid);
        self.open = false;
        self.ddl.clear();
        self.events.clear();
        self.part_rows = 0;
        self.part_bytes = 0;
        self.parts_sent = 0;
    }

    /// The transaction is in brackets: its end is still to come.
    pub(crate) fn mark_open(&mut self) {
        self.open = true;
    }

    pub(crate) fn is_open(&self) -> bool {
        self.open
    }

    /// Row events buffered for the transaction that is pending.
    pub(crate) fn buffered_rows(&self) -> usize {
        self.events.len()
    }

    /// The executed set once the transaction `pending` is committed: the set
    /// the reader started from, every transaction it has passed on, and
    /// `pending`. `start` gives the set the reader started from; it is asked
    /// once.
    pub(crate) fn set_after(
        &mut self,
        pending: &str,
        start: impl FnOnce() -> Result<ExecutedSet>,
    ) -> Result<String> {
        self.reader_set(start)?.gtid_set_after(pending)
    }

    /// As [`Self::set_after`], and `pending` counts as passed on from here.
    pub(crate) fn pass_on(
        &mut self,
        pending: &str,
        start: impl FnOnce() -> Result<ExecutedSet>,
    ) -> Result<String> {
        let set = self.reader_set(start)?;
        set.add_committed(pending)?;
        Ok(set.to_set_string())
    }

    fn reader_set(
        &mut self,
        start: impl FnOnce() -> Result<ExecutedSet>,
    ) -> Result<&mut ExecutedSet> {
        Ok(match &mut self.passed_on {
            Some(set) => set,
            none => none.insert(start()?),
        })
    }

    pub fn push_ddl(&mut self, stmt: DdlStatement) {
        self.ddl.push(stmt);
    }

    pub fn push_row(&mut self, event: CloudEvent) {
        self.push_row_sized(event, 0);
    }

    /// A row of about `bytes` bytes of row data.
    pub(crate) fn push_row_sized(&mut self, event: CloudEvent, bytes: usize) {
        self.events.push(event);
        self.part_rows += 1;
        self.part_bytes = self.part_bytes.saturating_add(bytes);
    }

    /// Bound the queue to the consumer by the bytes of its deliveries.
    pub(crate) fn with_queue(mut self, queue: Arc<QueueBytes>) -> Self {
        self.queue = Some(queue);
        self
    }

    /// Wait until `txn` fits in the queue to the consumer, and count it in.
    pub(crate) fn reserve_queue(
        &self,
        txn: &CommittedTransaction,
    ) -> std::result::Result<(), String> {
        match &self.queue {
            Some(queue) => queue.reserve(delivery_bytes(txn)),
            None => Ok(()),
        }
    }

    /// The rows buffered fill a part: deliver them before the next row.
    pub(crate) fn part_full(&self) -> bool {
        self.part_rows >= self.max_rows()
            || (self.part_rows > 0 && self.part_bytes >= self.max_bytes())
    }

    /// The rows buffered so far, as a part of the pending transaction that is
    /// not its last. It carries no position: the rows lose the executed set
    /// of the transaction (`gtidset`), which only the last part's rows carry,
    /// so that nothing in a part says the transaction is done.
    pub(crate) fn take_part(&mut self) -> Option<CommittedTransaction> {
        let gtid = self.pending_gtid.clone()?;
        self.parts_sent += 1;
        self.part_rows = 0;
        self.part_bytes = 0;
        let mut events = std::mem::take(&mut self.events);
        for event in &mut events {
            event.extensions.remove("gtidset");
        }
        Some(CommittedTransaction {
            gtid,
            gtid_set_after: String::new(),
            position: None,
            ddl: std::mem::take(&mut self.ddl),
            control: Vec::new(),
            events,
            part: Some(TransactionPart {
                number: self.parts_sent,
                last: false,
            }),
        })
    }

    /// Build commit payload at the end of the transaction; clears in-flight buffers but does not
    /// clear pending until taken.
    pub fn take_commit(&mut self, gtid_set_after: String) -> Option<CommittedTransaction> {
        let gtid = self.pending_gtid.take()?;
        self.open = false;
        // The last part of a transaction delivered in parts; a transaction
        // that fit in one part is delivered whole, without a part.
        let part = (self.parts_sent > 0).then_some(TransactionPart {
            number: self.parts_sent + 1,
            last: true,
        });
        self.parts_sent = 0;
        self.part_rows = 0;
        self.part_bytes = 0;
        Some(CommittedTransaction {
            part,
            gtid,
            gtid_set_after,
            position: None,
            ddl: std::mem::take(&mut self.ddl),
            control: Vec::new(),
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
        assert_eq!(txn.part, None, "a short transaction is delivered whole");
    }

    fn row_with_set() -> CloudEvent {
        let mut ext = serde_json::Map::new();
        ext.insert("gtidset".into(), serde_json::json!("uuid:1-1"));
        CloudEvent::row_change(
            "mysql://test",
            &TableRef::new("db", "t"),
            ChangeOp::Insert,
            serde_json::json!({"op": "insert"}),
            ext,
        )
    }

    #[test]
    fn a_long_transaction_is_cut_into_parts_by_rows() {
        let mut buf = TxnBuffer::with_part_limits(2, 1_000_000);
        buf.on_gtid("uuid:1".into());
        let mut parts = Vec::new();
        for _ in 0..5 {
            buf.push_row_sized(row_with_set(), 10);
            if buf.part_full() {
                parts.push(buf.take_part().expect("part"));
            }
        }
        let last = buf.take_commit("uuid:1-1".into()).expect("commit");
        assert_eq!(parts.len(), 2);
        for (i, part) in parts.iter().enumerate() {
            assert_eq!(part.gtid, "uuid:1");
            assert_eq!(part.gtid_set_after, "", "a part carries no position");
            assert_eq!(part.events.len(), 2);
            assert_eq!(
                part.part,
                Some(TransactionPart {
                    number: i as u32 + 1,
                    last: false
                })
            );
            assert!(!part.ends_transaction());
            assert!(part
                .events
                .iter()
                .all(|e| !e.extensions.contains_key("gtidset")));
        }
        assert_eq!(last.events.len(), 1);
        assert_eq!(last.gtid_set_after, "uuid:1-1");
        assert_eq!(
            last.part,
            Some(TransactionPart {
                number: 3,
                last: true
            })
        );
        assert!(last.ends_transaction());
        assert!(last.events[0].extensions.contains_key("gtidset"));
        // The next transaction starts counting again.
        buf.on_gtid("uuid:2".into());
        buf.push_row_sized(row_with_set(), 10);
        assert!(!buf.part_full());
        assert_eq!(
            buf.take_commit("uuid:1-2".into()).expect("commit").part,
            None
        );
    }

    #[test]
    fn a_part_also_ends_at_its_size_in_bytes() {
        let mut buf = TxnBuffer::with_part_limits(1_000, 100);
        buf.on_gtid("uuid:1".into());
        buf.push_row_sized(sample_row(), 60);
        assert!(!buf.part_full());
        buf.push_row_sized(sample_row(), 60);
        assert!(buf.part_full(), "120 bytes fill a part of 100");
        assert_eq!(buf.take_part().expect("part").events.len(), 2);
        // A row larger than a part is a part of its own.
        buf.push_row_sized(sample_row(), 500);
        assert!(buf.part_full());
        assert_eq!(buf.take_part().expect("part").events.len(), 1);
    }
}

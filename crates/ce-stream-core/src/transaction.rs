use serde::{Deserialize, Serialize};

use crate::event::CloudEvent;

/// One DDL statement observed in the binlog within a transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DdlStatement {
    pub schema: String,
    pub query: String,
}

/// A committed MySQL transaction — primary delivery unit when `delivery_unit = transaction`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommittedTransaction {
    /// GTID of this commit, e.g. `3e11fa47-71ca-11e1-9e33-c080020c9a66:1`
    pub gtid: String,
    /// Executed GTID set **after** this transaction commits (for checkpoint resume).
    pub gtid_set_after: String,
    /// DDL in binlog order, before row events.
    pub ddl: Vec<DdlStatement>,
    /// Row change events in binlog order.
    pub events: Vec<CloudEvent>,
}

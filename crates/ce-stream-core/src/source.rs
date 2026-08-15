use async_trait::async_trait;

use crate::error::Result;
use crate::event::{CloudEvent, PayloadMode, TableRef};
use crate::transaction::CommittedTransaction;

/// What the consumer receives per MySQL commit (after XID).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeliveryUnit {
    /// N row CloudEvents per commit, emitted in order **after** XID (default).
    #[default]
    Row,
    /// One [`CommittedTransaction`] envelope per commit.
    Transaction,
}

/// Common knobs every DB adapter understands.
#[derive(Debug, Clone)]
pub struct SourceConfig {
    /// Logical source id for CloudEvents `source` (e.g. `mysql://host:3306/ce-stream`).
    pub source_id: String,
    pub include_tables: Vec<TableRef>,
    /// Full row images vs signal-only.
    pub payload_mode: PayloadMode,
    /// Bounded event queue capacity (backpressure when sink is slow). Default 64.
    pub queue_capacity: usize,
    /// Per-row vs per-transaction delivery after commit.
    pub delivery_unit: DeliveryUnit,
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self {
            source_id: String::new(),
            include_tables: Vec::new(),
            payload_mode: PayloadMode::Full,
            queue_capacity: 64,
            delivery_unit: DeliveryUnit::Row,
        }
    }
}

#[async_trait]
pub trait ChangeSource: Send {
    /// Row mode (`delivery_unit = Row`): after XID, one callback per row CloudEvent.
    /// Checkpoint advances only after all row callbacks for the commit return Ok.
    async fn run<F>(&mut self, on_event: F) -> Result<()>
    where
        F: FnMut(CloudEvent) -> Result<()> + Send;

    /// Transaction mode (`delivery_unit = Transaction`): after XID, one envelope per commit.
    async fn run_transactions<F>(&mut self, on_txn: F) -> Result<()>
    where
        F: FnMut(CommittedTransaction) -> Result<()> + Send;
}

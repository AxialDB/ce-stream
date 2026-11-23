use async_trait::async_trait;

use crate::delivery::{CheckpointMode, Confirm, Delivery};
use crate::error::Result;
use crate::event::{CloudEvent, PayloadMode, TableRef};
use crate::gate::GateReport;
use crate::transaction::SourcePosition;

/// What the consumer receives per source commit (MySQL: after XID).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeliveryUnit {
    /// N row CloudEvents per commit, emitted in order **after** the commit (default).
    #[default]
    Row,
    /// One [`Delivery`] per commit, position passed, or table definition.
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
    /// A transaction is delivered in parts of at most this many rows
    /// ([`crate::TransactionPart`]). Default 10,000.
    pub part_max_rows: usize,
    /// ... and at most this many bytes of row data, whichever comes first. A
    /// single row larger than this is a part of its own. Default 16 MB.
    pub part_max_bytes: usize,
    /// The deliveries waiting for the consumer hold at most this many bytes of
    /// row data, besides `queue_capacity` deliveries. One delivery always
    /// fits an empty queue. Default 32 MB (two parts). 0 is the default.
    pub queue_max_bytes: usize,
    /// When the checkpoint moves: on the callback's `Ok`, or on
    /// [`Confirm::confirm`].
    pub checkpoint: CheckpointMode,
}

/// Default of [`SourceConfig::part_max_rows`].
pub const DEFAULT_PART_MAX_ROWS: usize = 10_000;
/// Default of [`SourceConfig::part_max_bytes`]: 16 MB.
pub const DEFAULT_PART_MAX_BYTES: usize = 16 * 1024 * 1024;
/// Default of [`SourceConfig::queue_max_bytes`]: 32 MB, two parts.
pub const DEFAULT_QUEUE_MAX_BYTES: usize = 2 * DEFAULT_PART_MAX_BYTES;

impl Default for SourceConfig {
    fn default() -> Self {
        Self {
            source_id: String::new(),
            include_tables: Vec::new(),
            payload_mode: PayloadMode::Full,
            queue_capacity: 64,
            delivery_unit: DeliveryUnit::Row,
            part_max_rows: DEFAULT_PART_MAX_ROWS,
            part_max_bytes: DEFAULT_PART_MAX_BYTES,
            queue_max_bytes: DEFAULT_QUEUE_MAX_BYTES,
            checkpoint: CheckpointMode::OnOk,
        }
    }
}

#[async_trait]
pub trait ChangeSource: Send + Sync {
    /// Row mode (`delivery_unit = Row`): after each commit, one callback per row CloudEvent.
    /// A table definition is a CloudEvent of its own ([`crate::delivery::SCHEMA_CE_TYPE`]).
    /// A position passed is not delivered in this mode; it still moves the checkpoint.
    /// Checkpoint advances only after all row callbacks for the commit return Ok.
    async fn run<F>(&mut self, on_event: F) -> Result<()>
    where
        F: FnMut(CloudEvent) -> Result<()> + Send;

    /// Transaction mode (`delivery_unit = Transaction`): one [`Delivery`] at a time.
    /// Under [`CheckpointMode::OnOk`] the checkpoint advances only after the
    /// callback returns Ok; under [`CheckpointMode::OnConfirm`], on [`Confirm::confirm`].
    async fn run_deliveries<F>(&mut self, on_delivery: F) -> Result<()>
    where
        F: FnMut(Delivery) -> Result<()> + Send;

    /// What the server and the included tables say about capture. `Ok` with
    /// failures in the report: a source about to run refuses on them unless
    /// its gate check is skipped. `Err` when the gate could not be asked.
    async fn gate(&self) -> Result<GateReport>;

    /// The handle for [`CheckpointMode::OnConfirm`]. Same handle every call.
    fn confirm(&self) -> Confirm;

    /// True when a stream that has delivered everything up to `at` has delivered
    /// everything up to `target`. `at` is a position the consumer received: a
    /// commit's `after`, or a `Progress` position. A part that is not the last
    /// has no `after` and answers `Err`.
    fn covers(&self, at: &SourcePosition, target: &SourcePosition) -> Result<bool>;
}

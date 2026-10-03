//! Shared types: CloudEvents envelope, change ops, source/sink traits, checkpoint.

// `async_trait` keeps rustc's message-less `#[must_use]` on methods that return
// `Pin<Box<dyn Future>>`, which is already `#[must_use]`. Clippy 1.99
// (`double_must_use`) rejects that. Drop this when async-trait allows the lint.
#![allow(clippy::double_must_use)]

pub mod avro_encode;
pub mod checkpoint;
pub mod error;
pub mod event;
pub mod include;
pub mod sink;
pub mod sinks;
pub mod source;
pub mod transaction;

pub use checkpoint::{Checkpoint, CheckpointStore};
pub use error::Error;
pub use event::{ChangeOp, CloudEvent, PayloadMode, SinkFormat, TableRef};
pub use include::{IncludeFilter, IncludeList};
pub use sink::Sink;
pub use sinks::{HttpSink, StdoutSink};
pub use source::{ChangeSource, DeliveryUnit, SourceConfig};
pub use transaction::{
    CommittedTransaction, ControlEvent, ControlKind, DdlStatement, SourcePosition,
};

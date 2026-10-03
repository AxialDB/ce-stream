//! MongoDB 8.0+ change streams → [`ce_stream_core::CloudEvent`].
//!
//! The stream is one cursor on the source database. Include-list changes are applied in this
//! process, so adding a collection does not reopen the cursor. Live tests are not part of CI.

// See `ce-stream-core`: async_trait vs clippy 1.99 `double_must_use`.
#![allow(clippy::double_must_use)]

mod bson_json;
mod checkpoint;
mod gate;
mod group;
mod map;
mod options;
mod source;

pub use checkpoint::{ClusterTime, MongoCheckpoint};
pub use options::FullDocumentMode;
pub use source::{MongoChangeStreamSource, MongoSourceOptions};

/// MongoDB `ChangeStreamHistoryLost`.
pub const HISTORY_LOST_CODE: i32 = 286;

//! MongoDB 8.0+ change streams → [`ce_stream_core::CloudEvent`].
//!
//! The stream is one cursor on the source database. A finite include list is sent to the server
//! as a `$match`, so writes to other collections are neither looked up nor sent. When the list
//! gains a collection the cursor is opened again at its last position with the wider filter;
//! [`ce_stream_core::include::IncludeList::in_effect_allows`] says when that has happened.
//! Live tests are not part of CI.

// See `ce-stream-core`: async_trait vs clippy 1.99 `double_must_use`.
#![allow(clippy::double_must_use)]

mod bson_json;
mod checkpoint;
mod gate;
mod group;
mod map;
mod options;
mod seed;
mod source;

pub use checkpoint::{ClusterTime, MongoCheckpoint};
pub use options::FullDocumentMode;
pub use seed::{note_cluster_time, open_seed_cursor, read_seed_batch, SEED_BATCH};
pub use source::{MongoChangeStreamSource, MongoSourceOptions};

/// MongoDB `ChangeStreamHistoryLost`.
pub const HISTORY_LOST_CODE: i32 = 286;

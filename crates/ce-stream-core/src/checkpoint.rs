use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::Result;

/// Opaque, adapter-specific resume token (MySQL: GTID set JSON; Mongo: resume token JSON).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Checkpoint {
    pub adapter: String,
    pub payload: serde_json::Value,
}

#[async_trait]
pub trait CheckpointStore: Send + Sync {
    async fn load(&self) -> Result<Option<Checkpoint>>;
    async fn save(&self, checkpoint: &Checkpoint) -> Result<()>;
}

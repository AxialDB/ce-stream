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

/// File-backed checkpoint, for any adapter.
pub struct FileCheckpointStore {
    pub path: std::path::PathBuf,
}

#[async_trait]
impl CheckpointStore for FileCheckpointStore {
    async fn load(&self) -> Result<Option<Checkpoint>> {
        if !self.path.exists() {
            return Ok(None);
        }
        let bytes = tokio::fs::read(&self.path).await?;
        let cp: Checkpoint = serde_json::from_slice(&bytes)?;
        Ok(Some(cp))
    }

    async fn save(&self, checkpoint: &Checkpoint) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let bytes = serde_json::to_vec_pretty(checkpoint)?;
        tokio::fs::write(&self.path, bytes).await?;
        Ok(())
    }
}

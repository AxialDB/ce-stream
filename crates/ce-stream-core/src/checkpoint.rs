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
    /// A save may be called once per commit, so it need not reach the disk: it
    /// must survive the process, not the machine.
    async fn save(&self, checkpoint: &Checkpoint) -> Result<()>;

    /// The last save survives the machine too. An adapter whose server forgets
    /// what is confirmed (a replication slot) calls this before it confirms.
    async fn sync(&self) -> Result<()> {
        Ok(())
    }
}

/// File-backed checkpoint, for any adapter. A crash leaves the old checkpoint or
/// the new one, never part of one.
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
        let mut temporary = self.path.clone().into_os_string();
        temporary.push(".tmp");
        let temporary = std::path::PathBuf::from(temporary);
        tokio::fs::write(&temporary, bytes).await?;
        tokio::fs::rename(&temporary, &self.path).await?;
        Ok(())
    }

    async fn sync(&self) -> Result<()> {
        if !self.path.exists() {
            return Ok(());
        }
        // Windows flushes a file only through a handle that can write it.
        tokio::fs::OpenOptions::new()
            .write(true)
            .open(&self.path)
            .await?
            .sync_all()
            .await?;
        // The rename is durable once its directory is. Windows has no such call.
        #[cfg(unix)]
        if let Some(parent) = self.path.parent().filter(|p| !p.as_os_str().is_empty()) {
            tokio::fs::File::open(parent).await?.sync_all().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_save_replaces_the_checkpoint_and_leaves_nothing_beside_it() {
        let dir = std::env::temp_dir().join(format!("ce-stream-cp-{}", uuid::Uuid::new_v4()));
        let store = FileCheckpointStore {
            path: dir.join("nested").join("cp.json"),
        };
        assert!(store.load().await.unwrap().is_none());
        for lsn in ["0/1", "0/2"] {
            store
                .save(&Checkpoint {
                    adapter: "t".into(),
                    payload: serde_json::json!({ "lsn": lsn }),
                })
                .await
                .unwrap();
        }
        store.sync().await.unwrap();
        assert_eq!(store.load().await.unwrap().unwrap().payload["lsn"], "0/2");
        let names: Vec<_> = std::fs::read_dir(dir.join("nested"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(names, ["cp.json"]);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

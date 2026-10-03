use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("source: {0}")]
    Source(String),
    #[error("sink: {0}")]
    Sink(String),
    #[error("checkpoint: {0}")]
    Checkpoint(String),
    #[error("config: {0}")]
    Config(String),
    /// The resume token is older than the oplog window. The consumer must seed again.
    #[error("change stream history lost: {0}")]
    HistoryLost(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

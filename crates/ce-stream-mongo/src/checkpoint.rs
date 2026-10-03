//! Mongo checkpoint payload. `after` is what a restart passes to `resume_after`.

use ce_stream_core::Checkpoint;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub const ADAPTER: &str = "mongo";

/// oplog cluster time, seconds plus increment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterTime {
    pub t: u32,
    pub i: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MongoCheckpoint {
    pub resume_token: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_time: Option<ClusterTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed_cluster_time: Option<ClusterTime>,
}

impl MongoCheckpoint {
    pub fn to_checkpoint(&self) -> Checkpoint {
        Checkpoint {
            adapter: ADAPTER.into(),
            payload: serde_json::to_value(self).unwrap_or(json!({})),
        }
    }

    pub fn from_checkpoint(cp: &Checkpoint) -> Option<Self> {
        if cp.adapter != ADAPTER {
            return None;
        }
        serde_json::from_value(cp.payload.clone()).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_keeps_seed_time() {
        let stored = MongoCheckpoint {
            resume_token: json!({"_data": "8264"}),
            cluster_time: Some(ClusterTime { t: 5, i: 1 }),
            seed_cluster_time: Some(ClusterTime { t: 4, i: 0 }),
        };
        let back = MongoCheckpoint::from_checkpoint(&stored.to_checkpoint()).unwrap();
        assert_eq!(back, stored);
    }

    #[test]
    fn other_adapter_is_ignored() {
        let cp = Checkpoint {
            adapter: "mysql".into(),
            payload: json!({"gtid": "sid:1"}),
        };
        assert!(MongoCheckpoint::from_checkpoint(&cp).is_none());
    }
}

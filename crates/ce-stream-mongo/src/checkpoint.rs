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

impl ClusterTime {
    /// The cluster time a resume token stands at. A token's `_data` is hex: one type byte
    /// (`82`, a timestamp), four bytes of seconds, four bytes of increment, then the rest.
    /// `None` for any other shape.
    pub fn of_resume_token(token: &Value) -> Option<Self> {
        let data = token.get("_data")?.as_str()?;
        if data.len() < 18 || !data.starts_with("82") {
            return None;
        }
        Some(Self {
            t: u32::from_str_radix(data.get(2..10)?, 16).ok()?,
            i: u32::from_str_radix(data.get(10..18)?, 16).ok()?,
        })
    }
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
    fn a_resume_token_gives_its_cluster_time() {
        // Tokens from MongoDB 8.0.32: an insert, and the same instant's fourth operation.
        let token = json!({"_data": "826AC91BAF000000012B042C0100296E5A1004A41BDC1AD3E14C8483C2360A39787616463C6F7065726174696F6E54797065003C696E736572740046646F63756D656E744B657900463C5F6964003C613300000004"});
        assert_eq!(
            ClusterTime::of_resume_token(&token),
            Some(ClusterTime {
                t: 0x6AC9_1BAF,
                i: 1
            })
        );
        let later = json!({"_data": "826AC91BAF000000042B042C0100296E5A1004"});
        assert_eq!(
            ClusterTime::of_resume_token(&later),
            Some(ClusterTime {
                t: 0x6AC9_1BAF,
                i: 4
            })
        );
        assert_eq!(
            ClusterTime::of_resume_token(&json!({"_data": "8264"})),
            None
        );
        assert_eq!(
            ClusterTime::of_resume_token(&json!({"_data": "zz6AC91BAF00000001"})),
            None
        );
        assert_eq!(ClusterTime::of_resume_token(&json!({})), None);
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

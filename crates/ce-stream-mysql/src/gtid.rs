//! Interval-aware executed GTID set (checkpoint resume watermark).

use ce_stream_core::error::{Error, Result};
use mysql_binlog_connector_rust::command::gtid_set::{GtidSet, Interval, UuidSet};

/// Durable executed GTID set loaded from / saved to checkpoint.
#[derive(Debug)]
pub struct ExecutedSet {
    inner: GtidSet,
}

impl Default for ExecutedSet {
    fn default() -> Self {
        Self::from_set_string("").expect("empty gtid set parses")
    }
}

impl ExecutedSet {
    pub fn from_set_string(s: &str) -> Result<Self> {
        let inner = GtidSet::new(s).map_err(|e| Error::Source(format!("gtid set parse: {e}")))?;
        Ok(Self { inner })
    }

    pub fn to_set_string(&self) -> String {
        self.inner.to_string()
    }

    /// Merge one committed GTID into the executed set.
    pub fn add_committed(&mut self, gtid: &str) -> Result<()> {
        self.inner
            .add(gtid)
            .map_err(|e| Error::Source(format!("gtid set add: {e}")))?;
        Ok(())
    }

    /// Executed set **after** merging `pending_gtid`, without mutating `self`.
    pub fn gtid_set_after(&self, pending_gtid: &str) -> Result<String> {
        let mut snapshot = ExecutedSet::from_set_string(&self.to_set_string())?;
        snapshot.add_committed(pending_gtid)?;
        Ok(snapshot.to_set_string())
    }

    /// GTID set for COM_BINLOG_DUMP_GTID: purged history + connect baseline + checkpoint watermark.
    pub fn binlog_handshake_gtid_set(
        checkpoint_gtid: &str,
        baseline_gtid: Option<&str>,
        gtid_purged: &str,
    ) -> Result<String> {
        if let Some(baseline) = baseline_gtid.filter(|s| !s.is_empty()) {
            return union_gtid_sets(&[gtid_purged, baseline, checkpoint_gtid]);
        }
        legacy_handshake_without_baseline(checkpoint_gtid, gtid_purged)
    }
}

/// Union GTID set strings (interval merge per server UUID).
pub fn union_gtid_sets(parts: &[&str]) -> Result<String> {
    use std::collections::HashMap;

    let mut by_uuid: HashMap<String, Vec<Interval>> = HashMap::new();
    for part in parts {
        if part.trim().is_empty() {
            continue;
        }
        let set = GtidSet::new(part).map_err(|e| Error::Source(format!("gtid set parse: {e}")))?;
        for uuid_set in set.get_uuid_sets() {
            by_uuid
                .entry(uuid_set.uuid.clone())
                .or_default()
                .extend(uuid_set.intervals.iter().cloned());
        }
    }

    let mut merged = GtidSet::new("").map_err(|e| Error::Source(format!("gtid set parse: {e}")))?;
    for (uuid, intervals) in by_uuid {
        let joined = merge_intervals(intervals);
        merged.put_uuid_set(UuidSet::new(uuid, joined));
    }
    Ok(merged.to_string())
}

fn legacy_handshake_without_baseline(checkpoint_gtid: &str, gtid_purged: &str) -> Result<String> {
    let Some((uuid, max)) = max_transaction_id(checkpoint_gtid)? else {
        return union_gtid_sets(&[gtid_purged, checkpoint_gtid]);
    };
    let filled = format!("{uuid}:1-{max}");
    union_gtid_sets(&[gtid_purged, &filled])
}

fn max_transaction_id(gtid_set: &str) -> Result<Option<(String, u64)>> {
    if gtid_set.trim().is_empty() {
        return Ok(None);
    }
    let set = GtidSet::new(gtid_set).map_err(|e| Error::Source(format!("gtid set parse: {e}")))?;
    let mut best: Option<(String, u64)> = None;
    for uuid_set in set.get_uuid_sets() {
        for interval in &uuid_set.intervals {
            let candidate = (uuid_set.uuid.clone(), interval.end);
            if best.as_ref().is_none_or(|(_, end)| interval.end > *end) {
                best = Some(candidate);
            }
        }
    }
    Ok(best)
}

fn merge_intervals(mut intervals: Vec<Interval>) -> Vec<Interval> {
    if intervals.is_empty() {
        return intervals;
    }
    intervals.sort_by_key(|i| i.start);
    let mut out = vec![intervals[0].clone()];
    for next in intervals.into_iter().skip(1) {
        let last = out.last_mut().expect("non-empty out");
        if next.start <= last.end + 1 {
            last.end = last.end.max(next.end);
        } else {
            out.push(next);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_preserves_holes() {
        let set = ExecutedSet::from_set_string("abc:1-10:12-15").unwrap();
        assert_eq!(set.to_set_string(), "abc:1-10:12-15");
    }

    #[test]
    fn add_committed_fills_hole() {
        let mut set = ExecutedSet::from_set_string("abc:1-10:12-15").unwrap();
        set.add_committed("abc:11").unwrap();
        assert_eq!(set.to_set_string(), "abc:1-15");
    }

    #[test]
    fn gtid_set_after_includes_pending() {
        let set = ExecutedSet::from_set_string("abc:1-10").unwrap();
        assert_eq!(set.gtid_set_after("abc:11").unwrap(), "abc:1-11");
    }

    #[test]
    fn duplicate_add_committed_is_idempotent() {
        let mut set = ExecutedSet::from_set_string("abc:1-10").unwrap();
        set.add_committed("abc:5").unwrap();
        assert_eq!(set.to_set_string(), "abc:1-10");
    }

    #[test]
    fn union_merges_purged_baseline_and_checkpoint() {
        let uuid = "49b103aa-6814-11f1-aab9-144fd7d9c421";
        let out = union_gtid_sets(&[
            &format!("{uuid}:1-242"),
            &format!("{uuid}:1-4104"),
            &format!("{uuid}:4105-4105"),
        ])
        .unwrap();
        assert_eq!(out, format!("{uuid}:1-4105"));
    }

    #[test]
    fn legacy_handshake_fills_through_checkpoint_high_water() {
        let uuid = "49b103aa-6814-11f1-aab9-144fd7d9c421";
        let out =
            ExecutedSet::binlog_handshake_gtid_set(&format!("{uuid}:4105-4105"), None, &format!("{uuid}:1-242"))
                .unwrap();
        assert_eq!(out, format!("{uuid}:1-4105"));
    }

    #[test]
    fn handshake_prefers_baseline_over_legacy_fill() {
        let uuid = "49b103aa-6814-11f1-aab9-144fd7d9c421";
        let out = ExecutedSet::binlog_handshake_gtid_set(
            &format!("{uuid}:4105-4105"),
            Some(&format!("{uuid}:1-4104")),
            &format!("{uuid}:1-242"),
        )
        .unwrap();
        assert_eq!(out, format!("{uuid}:1-4105"));
    }
}

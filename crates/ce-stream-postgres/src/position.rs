//! Positions: `at = {lsn, xid}` for a commit, `after = {lsn: <end LSN>}`.
//!
//! A commit record can start exactly where the one before it ended, so a commit
//! is compared by its end, never by its start against another commit's end.

use base64::Engine;
use ce_stream_core::delivery::has_resume_payload;
use ce_stream_core::error::{Error, Result};
use ce_stream_core::transaction::SourcePosition;
use serde_json::{json, Value};

use crate::lsn::Lsn;

pub const ADAPTER: &str = "postgres";

pub(crate) fn commit_at(lsn: Lsn, xid: u32) -> Value {
    json!({ "lsn": lsn.to_string(), "xid": xid })
}

pub(crate) fn after(lsn: Lsn) -> Value {
    json!({ "lsn": lsn.to_string() })
}

pub(crate) fn commit_position(lsn: Lsn, xid: u32, end_lsn: Option<Lsn>) -> SourcePosition {
    SourcePosition {
        adapter: ADAPTER.into(),
        at: commit_at(lsn, xid),
        after: end_lsn.map_or_else(|| json!({}), after),
    }
}

/// A position the stream passed with nothing in it.
pub(crate) fn passed(lsn: Lsn) -> SourcePosition {
    SourcePosition {
        adapter: ADAPTER.into(),
        at: after(lsn),
        after: after(lsn),
    }
}

/// A non-transactional message of the consumer's, at its LSN: where its record
/// ends, in the stream and from `pg_logical_emit_message` alike.
pub(crate) fn message_position(lsn: Lsn, prefix: &str, content: &[u8]) -> SourcePosition {
    SourcePosition {
        adapter: ADAPTER.into(),
        at: json!({
            "lsn": lsn.to_string(),
            "prefix": prefix,
            "content": base64::engine::general_purpose::STANDARD.encode(content),
        }),
        after: after(lsn),
    }
}

fn lsn_of(value: &Value, what: &str) -> Result<Lsn> {
    let text = value["lsn"]
        .as_str()
        .ok_or_else(|| Error::Source(format!("{what}: no lsn in {value}")))?;
    Lsn::parse(text)
}

/// The LSN a position resumes from.
pub(crate) fn after_lsn(position: &SourcePosition) -> Result<Lsn> {
    lsn_of(&position.after, "position after")
}

/// See [`ce_stream_core::ChangeSource::covers`].
pub fn covers_position(at: &SourcePosition, target: &SourcePosition) -> Result<bool> {
    for position in [at, target] {
        if position.adapter != ADAPTER {
            return Err(Error::Source(format!(
                "covers: a {} position on a {ADAPTER} source",
                position.adapter
            )));
        }
    }
    if !has_resume_payload(at) {
        return Err(Error::Source(
            "covers: a part that is not the last has no position after it".into(),
        ));
    }
    let reached = after_lsn(at)?;
    if has_resume_payload(target) {
        return Ok(after_lsn(target)? <= reached);
    }
    // A part of a transaction: covered once a position is past its commit's start.
    Ok(lsn_of(&target.at, "position at")? < reached)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn commit(lsn: u64, end: u64) -> SourcePosition {
        commit_position(Lsn(lsn), 1, Some(Lsn(end)))
    }

    #[test]
    fn a_commit_is_covered_by_its_own_end_and_anything_later() {
        assert!(covers_position(&commit(10, 20), &commit(10, 20)).unwrap());
        assert!(covers_position(&commit(30, 40), &commit(10, 20)).unwrap());
        assert!(covers_position(&passed(Lsn(20)), &commit(10, 20)).unwrap());
        assert!(!covers_position(&passed(Lsn(19)), &commit(10, 20)).unwrap());
    }

    #[test]
    fn a_commit_that_starts_where_the_last_one_ended_is_not_covered_by_it() {
        assert!(!covers_position(&commit(10, 20), &commit(20, 30)).unwrap());
    }

    #[test]
    fn a_mark_is_covered_by_its_own_message_and_anything_at_or_past_it() {
        let mark = message_position(Lsn(150), "p", b"id");
        assert!(covers_position(&message_position(Lsn(150), "p", b"id"), &mark).unwrap());
        assert!(covers_position(&commit(150, 180), &mark).unwrap());
        assert!(covers_position(&passed(Lsn(150)), &mark).unwrap());
        assert!(!covers_position(&passed(Lsn(149)), &mark).unwrap());
    }

    #[test]
    fn a_part_answers_err_as_at_and_is_measured_by_its_commit_as_target() {
        let part = commit_position(Lsn(10), 1, None);
        assert!(covers_position(&part, &commit(1, 2)).is_err());
        assert!(covers_position(&commit(10, 20), &part).unwrap());
        assert!(!covers_position(&passed(Lsn(10)), &part).unwrap());
    }

    #[test]
    fn another_adapters_position_is_an_error() {
        let mut other = commit(1, 2);
        other.adapter = "mysql".into();
        assert!(covers_position(&other, &commit(1, 2)).is_err());
    }
}

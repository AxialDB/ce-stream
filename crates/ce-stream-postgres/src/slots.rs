//! Slots are the embedder's to make and drop: the stream never makes one.

use ce_stream_core::error::{Error, Result};
use serde::{Deserialize, Serialize};

use crate::lsn::Lsn;
use crate::options::PostgresOptions;
use crate::wire::{quote_ident, quote_literal, Connection};

/// What the server says about a slot, for a health display.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotFigures {
    pub slot_name: String,
    pub active: bool,
    /// `reserved`, `extended`, `unreserved`, or `lost`.
    pub wal_status: Option<String>,
    /// Bytes the slot holds back, from its restart LSN.
    pub bytes_held: Option<i64>,
    pub safe_wal_size: Option<i64>,
    pub confirmed_flush_lsn: Option<String>,
    pub conflicting: Option<bool>,
    pub invalidation_reason: Option<String>,
}

impl SlotFigures {
    /// Why this slot can deliver no more changes, if it cannot.
    pub fn lost_because(&self) -> Option<String> {
        if let Some(reason) = &self.invalidation_reason {
            return Some(format!("invalidated: {reason}"));
        }
        if self.conflicting == Some(true) {
            return Some("invalidated by a recovery conflict".into());
        }
        (self.wal_status.as_deref() == Some("lost")).then(|| "its WAL is gone".to_string())
    }
}

pub(crate) fn figures_sql(slot: &str) -> String {
    format!(
        "SELECT json_build_object(
            'slot_name', slot_name,
            'active', active,
            'wal_status', wal_status,
            'bytes_held', pg_wal_lsn_diff(
                CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn()
                     ELSE pg_current_wal_lsn() END, restart_lsn)::bigint,
            'safe_wal_size', safe_wal_size,
            'confirmed_flush_lsn', confirmed_flush_lsn::text,
            'conflicting', conflicting,
            'invalidation_reason', invalidation_reason)::text
         FROM pg_replication_slots WHERE slot_name = {}",
        quote_literal(slot)
    )
}

pub(crate) async fn read_figures(conn: &mut Connection, slot: &str) -> Result<Option<SlotFigures>> {
    let rows = conn.query(&figures_sql(slot)).await?;
    let Some(text) = rows
        .into_iter()
        .next()
        .and_then(|row| row.into_iter().next().flatten())
    else {
        return Ok(None);
    };
    Ok(Some(serde_json::from_str(&text)?))
}

/// The slot's figures; `None` when there is no such slot.
pub async fn slot_figures(options: &PostgresOptions, slot: &str) -> Result<Option<SlotFigures>> {
    read_figures(&mut Connection::connect(options, false).await?, slot).await
}

/// Make a logical slot. The stream starts at the LSN returned. On a standby this
/// waits until `pg_log_standby_snapshot()` runs on the primary.
pub async fn create_slot(options: &PostgresOptions, slot: &str) -> Result<Lsn> {
    let mut conn = Connection::connect(options, true).await?;
    let rows = conn
        .query(&format!(
            "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput (SNAPSHOT 'nothing')",
            quote_ident(slot)
        ))
        .await?;
    let point = rows
        .first()
        .and_then(|row| row.get(1))
        .and_then(Option::as_deref)
        .ok_or_else(|| Error::Source("CREATE_REPLICATION_SLOT returned no position".into()))?;
    Lsn::parse(point)
}

pub async fn drop_slot(options: &PostgresOptions, slot: &str) -> Result<()> {
    let mut conn = Connection::connect(options, true).await?;
    conn.query(&format!("DROP_REPLICATION_SLOT {}", quote_ident(slot)))
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn figures() -> SlotFigures {
        SlotFigures {
            slot_name: "s".into(),
            active: false,
            wal_status: Some("reserved".into()),
            bytes_held: Some(0),
            safe_wal_size: None,
            confirmed_flush_lsn: Some("0/10".into()),
            conflicting: Some(false),
            invalidation_reason: None,
        }
    }

    #[test]
    fn a_slot_is_lost_by_any_of_the_three_signs() {
        assert_eq!(figures().lost_because(), None);
        let mut lost = figures();
        lost.wal_status = Some("lost".into());
        assert!(lost.lost_because().is_some());
        let mut lost = figures();
        lost.conflicting = Some(true);
        assert!(lost.lost_because().is_some());
        let mut lost = figures();
        lost.invalidation_reason = Some("idle_timeout".into());
        assert!(lost.lost_because().unwrap().contains("idle_timeout"));
    }
}

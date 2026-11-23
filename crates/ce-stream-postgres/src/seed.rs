//! The cut for a seed, on the consumer's own connection to the primary: both
//! steps need it, since a message writes WAL and a fresh xid cannot be assigned
//! in recovery.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use ce_stream_core::delivery::SeedCut;
use ce_stream_core::error::{Error, Result};
use ce_stream_core::transaction::SourcePosition;

use crate::lsn::Lsn;
use crate::options::PostgresOptions;
use crate::position::message_position;
use crate::wire::{quote_literal, Connection};

const POLL: Duration = Duration::from_millis(100);
const BATCH: usize = 1000;

/// `mark` writes a non-transactional message under a prefix the stream delivers,
/// and returns its position, the same the stream delivers it at. `wait_settled`
/// waits for the transactions running when the mark was written.
pub struct PgSeedCut {
    options: PostgresOptions,
    prefix: String,
    /// What was running at each mark, by its LSN.
    in_flight: Mutex<HashMap<String, Vec<u64>>>,
}

impl PgSeedCut {
    /// `prefix` must be among the stream's `message_prefixes`.
    pub fn new(options: PostgresOptions, prefix: impl Into<String>) -> Self {
        Self {
            options,
            prefix: prefix.into(),
            in_flight: Mutex::new(HashMap::new()),
        }
    }
}

/// Every xid that can be running: the snapshot's in-progress list, and all from
/// its `xmax` to a fresh xid, since a running xid at or above `xmax` is not listed.
fn xids_to_watch(snapshot: &str, fresh: u64) -> Result<Vec<u64>> {
    let bad = || Error::Source(format!("bad snapshot {snapshot:?}"));
    let mut fields = snapshot.split(':');
    let _xmin = fields.next().ok_or_else(bad)?;
    let xmax: u64 = fields.next().ok_or_else(bad)?.parse().map_err(|_| bad())?;
    let listed = fields.next().ok_or_else(bad)?;
    let mut xids = listed
        .split(',')
        .filter(|xid| !xid.is_empty())
        .map(|xid| xid.parse::<u64>().map_err(|_| bad()))
        .collect::<Result<Vec<_>>>()?;
    xids.extend(xmax..fresh);
    Ok(xids)
}

fn single_text(rows: &[Vec<Option<String>>], what: &str) -> Result<String> {
    rows.first()
        .and_then(|row| row.first())
        .and_then(Clone::clone)
        .ok_or_else(|| Error::Source(format!("{what} returned nothing")))
}

async fn running_now(conn: &mut Connection) -> Result<Vec<u64>> {
    // The fresh xid is last: every xid below it has been assigned.
    let snapshot = single_text(
        &conn.query("SELECT pg_current_snapshot()::text").await?,
        "pg_current_snapshot",
    )?;
    let fresh = single_text(
        &conn.query("SELECT pg_current_xact_id()::text").await?,
        "pg_current_xact_id",
    )?;
    let fresh = fresh
        .parse()
        .map_err(|_| Error::Source(format!("bad xid {fresh:?}")))?;
    xids_to_watch(&snapshot, fresh)
}

#[async_trait]
impl SeedCut for PgSeedCut {
    async fn mark(&self) -> Result<SourcePosition> {
        let mut conn = Connection::connect(&self.options, false).await?;
        let content = uuid::Uuid::new_v4().to_string();
        let lsn = single_text(
            &conn
                .query(&format!(
                    "SELECT pg_logical_emit_message(false, {}, {})::text",
                    quote_literal(&self.prefix),
                    quote_literal(&content)
                ))
                .await?,
            "pg_logical_emit_message",
        )?;
        let lsn = Lsn::parse(&lsn)?;
        let running = running_now(&mut conn).await?;
        self.in_flight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(lsn.to_string(), running);
        Ok(message_position(lsn, &self.prefix, content.as_bytes()))
    }

    async fn wait_settled(&self, mark: &SourcePosition) -> Result<()> {
        let mut conn = Connection::connect(&self.options, false).await?;
        let key = mark.at["lsn"].as_str().unwrap_or_default();
        let held = self
            .in_flight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(key);
        // A mark from before a restart: what runs now includes what ran then.
        let mut waiting = match held {
            Some(xids) => xids,
            None => running_now(&mut conn).await?,
        };
        while !waiting.is_empty() {
            let mut still = Vec::new();
            for batch in waiting.chunks(BATCH) {
                let list = batch
                    .iter()
                    .map(|xid| format!("'{xid}'::xid8"))
                    .collect::<Vec<_>>()
                    .join(",");
                let rows = conn
                    .query(&format!(
                        "SELECT x::text FROM unnest(ARRAY[{list}]) AS x
                         WHERE pg_xact_status(x) = 'in progress'"
                    ))
                    .await?;
                for row in rows {
                    if let Some(Some(xid)) = row.into_iter().next() {
                        still.push(
                            xid.parse()
                                .map_err(|_| Error::Source(format!("bad xid {xid:?}")))?,
                        );
                    }
                }
            }
            waiting = still;
            if !waiting.is_empty() {
                tokio::time::sleep(POLL).await;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_list_and_everything_from_xmax_to_the_fresh_xid() {
        assert_eq!(
            xids_to_watch("100:105:100,103", 108).unwrap(),
            vec![100, 103, 105, 106, 107]
        );
        assert_eq!(xids_to_watch("5:5:", 6).unwrap(), vec![5]);
        assert_eq!(xids_to_watch("5:5:", 5).unwrap(), Vec::<u64>::new());
        assert!(xids_to_watch("nonsense", 6).is_err());
    }
}

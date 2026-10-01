//! Optional Avro encoding for CloudEvents and committed transactions.
//!
//! Schemas:
//! - `schemas/cloudevent-v1.avsc` (id `ce-stream.cloudevent.v1`)
//! - `schemas/committed-transaction-v1.avsc` (id `ce-stream.committed-transaction.v1`)
//!
//! Wire: single Avro datum (no OCF). Schema Registry is out of scope.

use std::sync::OnceLock;

use apache_avro::types::Value;
use apache_avro::{from_avro_datum, to_avro_datum, Schema};

use crate::error::{Error, Result};
use crate::event::CloudEvent;
use crate::transaction::{CommittedTransaction, DdlStatement};

/// Stable schema identifier for row CloudEvents (not a Confluent Schema Registry id).
pub const SCHEMA_ID: &str = "ce-stream.cloudevent.v1";

/// Stable schema identifier for committed transaction envelopes.
pub const COMMITTED_TRANSACTION_SCHEMA_ID: &str = "ce-stream.committed-transaction.v1";

/// HTTP Content-Type for Avro-encoded CloudEvents.
pub const CONTENT_TYPE_AVRO: &str = "application/cloudevents+avro";

/// HTTP Content-Type for Avro-encoded committed transactions.
pub const CONTENT_TYPE_COMMITTED_TXN_AVRO: &str =
    "application/ce-stream.committed-transaction+avro";

/// Embedded copy of `schemas/cloudevent-v1.avsc`.
pub const SCHEMA_JSON: &str = include_str!("../schemas/cloudevent-v1.avsc");

/// Embedded copy of `schemas/committed-transaction-v1.avsc`.
pub const COMMITTED_TRANSACTION_SCHEMA_JSON: &str =
    include_str!("../schemas/committed-transaction-v1.avsc");

fn cloudevent_schema() -> &'static Schema {
    static SCHEMA: OnceLock<Schema> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        Schema::parse_str(SCHEMA_JSON).expect("embedded cloudevent-v1.avsc must parse")
    })
}

fn committed_transaction_schema() -> &'static Schema {
    static SCHEMA: OnceLock<Schema> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        Schema::parse_str(COMMITTED_TRANSACTION_SCHEMA_JSON)
            .expect("embedded committed-transaction-v1.avsc must parse")
    })
}

fn cloud_event_to_value(event: &CloudEvent) -> Result<Value> {
    let extensions_json =
        serde_json::to_string(&event.extensions).map_err(|e| Error::Sink(e.to_string()))?;
    let data_json = serde_json::to_string(&event.data).map_err(|e| Error::Sink(e.to_string()))?;

    Ok(Value::Record(vec![
        (
            "specversion".into(),
            Value::String(event.specversion.clone()),
        ),
        ("id".into(), Value::String(event.id.clone())),
        ("type".into(), Value::String(event.ty.clone())),
        ("source".into(), Value::String(event.source.clone())),
        ("subject".into(), Value::String(event.subject.clone())),
        ("time".into(), Value::String(event.time.clone())),
        ("extensions_json".into(), Value::String(extensions_json)),
        ("data_json".into(), Value::String(data_json)),
    ]))
}

fn ddl_to_value(stmt: &DdlStatement) -> Value {
    Value::Record(vec![
        ("schema".into(), Value::String(stmt.schema.clone())),
        ("query".into(), Value::String(stmt.query.clone())),
    ])
}

fn get_string_field(fields: &[(String, Value)], name: &str) -> Result<String> {
    fields
        .iter()
        .find(|(k, _)| k == name)
        .and_then(|(_, v)| match v {
            Value::String(s) => Some(s.clone()),
            _ => None,
        })
        .ok_or_else(|| Error::Sink(format!("avro decode: missing string field {name}")))
}

fn cloud_event_from_record(fields: &[(String, Value)]) -> Result<CloudEvent> {
    let extensions: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(&get_string_field(fields, "extensions_json")?)
            .map_err(|e| Error::Sink(format!("avro decode extensions_json: {e}")))?;
    let data: serde_json::Value = serde_json::from_str(&get_string_field(fields, "data_json")?)
        .map_err(|e| Error::Sink(format!("avro decode data_json: {e}")))?;

    Ok(CloudEvent {
        specversion: get_string_field(fields, "specversion")?,
        id: get_string_field(fields, "id")?,
        ty: get_string_field(fields, "type")?,
        source: get_string_field(fields, "source")?,
        subject: get_string_field(fields, "subject")?,
        time: get_string_field(fields, "time")?,
        extensions,
        data,
    })
}

fn ddl_from_record(fields: &[(String, Value)]) -> Result<DdlStatement> {
    Ok(DdlStatement {
        schema: get_string_field(fields, "schema")?,
        query: get_string_field(fields, "query")?,
    })
}

/// Encode a CloudEvent as a single Avro binary datum.
pub fn encode_cloudevent(event: &CloudEvent) -> Result<Vec<u8>> {
    let value = cloud_event_to_value(event)?;
    to_avro_datum(cloudevent_schema(), value).map_err(|e| Error::Sink(format!("avro encode: {e}")))
}

/// Decode a single Avro datum back to a CloudEvent (tests / consumers).
pub fn decode_cloudevent(bytes: &[u8]) -> Result<CloudEvent> {
    let mut cursor = std::io::Cursor::new(bytes);
    let value = from_avro_datum(cloudevent_schema(), &mut cursor, None)
        .map_err(|e| Error::Sink(format!("avro decode: {e}")))?;

    let Value::Record(fields) = value else {
        return Err(Error::Sink("avro decode: expected record".into()));
    };

    cloud_event_from_record(&fields)
}

/// Encode a committed transaction as a single Avro binary datum.
///
/// `committed-transaction-v1` carries MySQL commits only. A commit with
/// `position` or `control` set is refused rather than encoded without them.
pub fn encode_committed_transaction(txn: &CommittedTransaction) -> Result<Vec<u8>> {
    if txn.position.is_some() || !txn.control.is_empty() {
        let adapter = txn.source_position().adapter;
        return Err(Error::Sink(format!(
            "avro committed-transaction-v1 carries MySQL commits only; use JSON for adapter {adapter}"
        )));
    }
    let ddl: Result<Vec<_>> = txn.ddl.iter().map(|s| Ok(ddl_to_value(s))).collect();
    let events: Result<Vec<_>> = txn.events.iter().map(cloud_event_to_value).collect();

    let value = Value::Record(vec![
        ("gtid".into(), Value::String(txn.gtid.clone())),
        (
            "gtid_set_after".into(),
            Value::String(txn.gtid_set_after.clone()),
        ),
        ("ddl".into(), Value::Array(ddl?)),
        ("events".into(), Value::Array(events?)),
    ]);

    to_avro_datum(committed_transaction_schema(), value)
        .map_err(|e| Error::Sink(format!("avro encode committed transaction: {e}")))
}

/// Decode a single Avro datum back to a committed transaction.
pub fn decode_committed_transaction(bytes: &[u8]) -> Result<CommittedTransaction> {
    let mut cursor = std::io::Cursor::new(bytes);
    let value = from_avro_datum(committed_transaction_schema(), &mut cursor, None)
        .map_err(|e| Error::Sink(format!("avro decode committed transaction: {e}")))?;

    let Value::Record(fields) = value else {
        return Err(Error::Sink(
            "avro decode committed transaction: expected record".into(),
        ));
    };

    let gtid = get_string_field(&fields, "gtid")?;
    let gtid_set_after = get_string_field(&fields, "gtid_set_after")?;

    let ddl = decode_ddl_array(&fields)?;
    let events = decode_events_array(&fields)?;

    Ok(CommittedTransaction {
        gtid,
        gtid_set_after,
        position: None,
        ddl,
        control: Vec::new(),
        events,
    })
}

fn decode_ddl_array(fields: &[(String, Value)]) -> Result<Vec<DdlStatement>> {
    let ddl_value = fields
        .iter()
        .find(|(k, _)| k == "ddl")
        .map(|(_, v)| v)
        .ok_or_else(|| Error::Sink("avro decode: missing ddl field".into()))?;

    let Value::Array(items) = ddl_value else {
        return Err(Error::Sink("avro decode: ddl must be array".into()));
    };

    items
        .iter()
        .map(|item| {
            let Value::Record(record) = item else {
                return Err(Error::Sink("avro decode: ddl item must be record".into()));
            };
            ddl_from_record(record)
        })
        .collect()
}

fn decode_events_array(fields: &[(String, Value)]) -> Result<Vec<CloudEvent>> {
    let events_value = fields
        .iter()
        .find(|(k, _)| k == "events")
        .map(|(_, v)| v)
        .ok_or_else(|| Error::Sink("avro decode: missing events field".into()))?;

    let Value::Array(items) = events_value else {
        return Err(Error::Sink("avro decode: events must be array".into()));
    };

    items
        .iter()
        .map(|item| {
            let Value::Record(record) = item else {
                return Err(Error::Sink(
                    "avro decode: events item must be record".into(),
                ));
            };
            cloud_event_from_record(record)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{ChangeOp, TableRef};

    fn sample_row_event(n: i32) -> CloudEvent {
        CloudEvent::row_change(
            "mysql://lab/ce-stream",
            &TableRef::new("db", "t1"),
            ChangeOp::Insert,
            serde_json::json!({"op": "insert", "after": {"id": n}}),
            serde_json::Map::new(),
        )
    }

    fn sample_committed_txn() -> CommittedTransaction {
        CommittedTransaction {
            gtid: "36682cf3-a048-11ed-b4b3-0242ac110004:42".into(),
            gtid_set_after: "36682cf3-a048-11ed-b4b3-0242ac110004:1-42".into(),
            ddl: vec![DdlStatement {
                schema: "app".into(),
                query: "ALTER TABLE t1 ADD COLUMN x INT".into(),
            }],
            events: vec![
                sample_row_event(1),
                sample_row_event(2),
                sample_row_event(3),
            ],
            ..Default::default()
        }
    }

    #[test]
    fn avro_refuses_non_mysql_commit() {
        let txn = CommittedTransaction {
            position: Some(crate::transaction::SourcePosition {
                adapter: "mongo".into(),
                at: serde_json::json!({}),
                after: serde_json::json!({}),
            }),
            ..Default::default()
        };
        let err = encode_committed_transaction(&txn).unwrap_err().to_string();
        assert!(err.contains("adapter mongo"), "{err}");
    }

    #[test]
    fn roundtrip_cloudevent_avro() {
        let mut ext = serde_json::Map::new();
        ext.insert("gtid".into(), "uuid:1-2-3".into());
        let ev = CloudEvent::row_change(
            "mysql://lab/ce-stream",
            &TableRef::new("db", "t1"),
            ChangeOp::Insert,
            serde_json::json!({"op": "insert", "after": {"id": 1}}),
            ext,
        );
        let bytes = encode_cloudevent(&ev).expect("encode");
        assert!(!bytes.is_empty());
        let back = decode_cloudevent(&bytes).expect("decode");
        assert_eq!(back.id, ev.id);
        assert_eq!(back.ty, ev.ty);
        assert_eq!(back.subject, "db.t1");
        assert_eq!(
            back.extensions.get("gtid").and_then(|v| v.as_str()),
            Some("uuid:1-2-3")
        );
        assert_eq!(back.data["op"], "insert");
    }

    #[test]
    fn roundtrip_committed_transaction_json() {
        let txn = sample_committed_txn();
        let json = serde_json::to_string(&txn).expect("json encode");
        let back: CommittedTransaction = serde_json::from_str(&json).expect("json decode");
        assert_eq!(back.gtid, txn.gtid);
        assert_eq!(back.ddl.len(), 1);
        assert_eq!(back.events.len(), 3);
    }

    #[test]
    fn roundtrip_committed_transaction_avro() {
        let txn = sample_committed_txn();
        let bytes = encode_committed_transaction(&txn).expect("encode");
        assert!(!bytes.is_empty());
        let back = decode_committed_transaction(&bytes).expect("decode");
        assert_eq!(back.gtid, txn.gtid);
        assert_eq!(back.gtid_set_after, txn.gtid_set_after);
        assert_eq!(back.ddl, txn.ddl);
        assert_eq!(back.events.len(), 3);
        assert_eq!(back.events[0].subject, "db.t1");
    }
}

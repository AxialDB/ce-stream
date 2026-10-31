//! Optional Avro encoding for CloudEvents, committed transactions, and the other deliveries.
//!
//! Schemas:
//! - `schemas/cloudevent-v1.avsc` (id `ce-stream.cloudevent.v1`)
//! - `schemas/committed-transaction-v4.avsc` (id `ce-stream.committed-transaction.v4`): what is written
//! - `schemas/progress-v1.avsc` (id `ce-stream.progress.v1`)
//! - `schemas/table-schema-v1.avsc` (id `ce-stream.table-schema.v1`)
//! - `schemas/committed-transaction-v1.avsc` to `-v3.avsc`: read, not written
//!
//! Wire: single Avro datum (no OCF). Schema Registry is out of scope.

use std::sync::OnceLock;

use apache_avro::types::Value;
use apache_avro::{from_avro_datum, to_avro_datum, Schema};

use crate::delivery::{ColumnSchema, Delivery, TableSchema};
use crate::error::{Error, Result};
use crate::event::{CloudEvent, TableRef};
use crate::transaction::{CommittedTransaction, DdlStatement, SourcePosition, TransactionPart};

/// Stable schema identifier for row CloudEvents (not a Confluent Schema Registry id).
pub const SCHEMA_ID: &str = "ce-stream.cloudevent.v1";

/// Stable schema identifier for MySQL committed transaction envelopes. Read, not written.
pub const COMMITTED_TRANSACTION_SCHEMA_ID: &str = "ce-stream.committed-transaction.v1";

/// Stable schema identifier for commits that carry a source position or control events.
/// Read, not written.
pub const COMMITTED_TRANSACTION_V2_SCHEMA_ID: &str = "ce-stream.committed-transaction.v2";

/// Stable schema identifier for a part of a transaction delivered in parts. Read, not written.
pub const COMMITTED_TRANSACTION_V3_SCHEMA_ID: &str = "ce-stream.committed-transaction.v3";

/// Stable schema identifier for a committed transaction from any adapter.
pub const COMMITTED_TRANSACTION_V4_SCHEMA_ID: &str = "ce-stream.committed-transaction.v4";

/// Stable schema identifier for [`Delivery::Progress`].
pub const PROGRESS_SCHEMA_ID: &str = "ce-stream.progress.v1";

/// Stable schema identifier for [`Delivery::Schema`].
pub const TABLE_SCHEMA_SCHEMA_ID: &str = "ce-stream.table-schema.v1";

/// HTTP Content-Type for Avro-encoded CloudEvents.
pub const CONTENT_TYPE_AVRO: &str = "application/cloudevents+avro";

/// HTTP Content-Type for Avro-encoded committed transactions.
pub const CONTENT_TYPE_COMMITTED_TXN_AVRO: &str =
    "application/ce-stream.committed-transaction+avro";

/// HTTP Content-Type for an Avro-encoded [`Delivery::Progress`].
pub const CONTENT_TYPE_PROGRESS_AVRO: &str = "application/ce-stream.progress+avro";

/// HTTP Content-Type for an Avro-encoded [`Delivery::Schema`].
pub const CONTENT_TYPE_TABLE_SCHEMA_AVRO: &str = "application/ce-stream.table-schema+avro";

/// Embedded copy of `schemas/cloudevent-v1.avsc`.
pub const SCHEMA_JSON: &str = include_str!("../schemas/cloudevent-v1.avsc");

/// Embedded copy of `schemas/committed-transaction-v1.avsc`.
pub const COMMITTED_TRANSACTION_SCHEMA_JSON: &str =
    include_str!("../schemas/committed-transaction-v1.avsc");

/// Embedded copy of `schemas/committed-transaction-v2.avsc`.
pub const COMMITTED_TRANSACTION_V2_SCHEMA_JSON: &str =
    include_str!("../schemas/committed-transaction-v2.avsc");

/// Embedded copy of `schemas/committed-transaction-v3.avsc`.
pub const COMMITTED_TRANSACTION_V3_SCHEMA_JSON: &str =
    include_str!("../schemas/committed-transaction-v3.avsc");

/// Embedded copy of `schemas/committed-transaction-v4.avsc`.
pub const COMMITTED_TRANSACTION_V4_SCHEMA_JSON: &str =
    include_str!("../schemas/committed-transaction-v4.avsc");

/// Embedded copy of `schemas/progress-v1.avsc`.
pub const PROGRESS_SCHEMA_JSON: &str = include_str!("../schemas/progress-v1.avsc");

/// Embedded copy of `schemas/table-schema-v1.avsc`.
pub const TABLE_SCHEMA_SCHEMA_JSON: &str = include_str!("../schemas/table-schema-v1.avsc");

macro_rules! embedded_schema {
    ($name:ident, $json:expr) => {
        fn $name() -> &'static Schema {
            static SCHEMA: OnceLock<Schema> = OnceLock::new();
            SCHEMA.get_or_init(|| Schema::parse_str($json).expect("embedded schema must parse"))
        }
    };
}

embedded_schema!(cloudevent_schema, SCHEMA_JSON);
embedded_schema!(
    committed_transaction_schema,
    COMMITTED_TRANSACTION_SCHEMA_JSON
);
embedded_schema!(
    committed_transaction_v2_schema,
    COMMITTED_TRANSACTION_V2_SCHEMA_JSON
);
embedded_schema!(
    committed_transaction_v3_schema,
    COMMITTED_TRANSACTION_V3_SCHEMA_JSON
);
embedded_schema!(
    committed_transaction_v4_schema,
    COMMITTED_TRANSACTION_V4_SCHEMA_JSON
);
embedded_schema!(progress_schema, PROGRESS_SCHEMA_JSON);
embedded_schema!(table_schema_schema, TABLE_SCHEMA_SCHEMA_JSON);

/// Schema id `encode_committed_transaction` uses, for every commit.
pub fn committed_transaction_schema_id(_txn: &CommittedTransaction) -> &'static str {
    COMMITTED_TRANSACTION_V4_SCHEMA_ID
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

fn field<'a>(fields: &'a [(String, Value)], name: &str) -> Option<&'a Value> {
    fields.iter().find(|(k, _)| k == name).map(|(_, v)| v)
}

fn get_string_field(fields: &[(String, Value)], name: &str) -> Result<String> {
    match field(fields, name) {
        Some(Value::String(s)) => Ok(s.clone()),
        _ => Err(Error::Sink(format!(
            "avro decode: missing string field {name}"
        ))),
    }
}

fn get_json_field(fields: &[(String, Value)], name: &str) -> Result<serde_json::Value> {
    serde_json::from_str(&get_string_field(fields, name)?)
        .map_err(|e| Error::Sink(format!("avro decode {name}: {e}")))
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

/// Encode a committed transaction as a single Avro binary datum, in
/// `committed-transaction-v4`. Row CloudEvents stay on `cloudevent-v1`.
pub fn encode_committed_transaction(txn: &CommittedTransaction) -> Result<Vec<u8>> {
    let (adapter, at, after) = match &txn.position {
        Some(position) => (
            position.adapter.clone(),
            serde_json::to_string(&position.at)?,
            serde_json::to_string(&position.after)?,
        ),
        None => (String::new(), "{}".to_string(), "{}".to_string()),
    };
    let (number, last) = match txn.part {
        Some(part) => (
            i32::try_from(part.number).map_err(|_| {
                Error::Sink(format!(
                    "avro encode: part number {} too large",
                    part.number
                ))
            })?,
            part.last,
        ),
        None => (0, false),
    };
    let control_json = serde_json::to_string(&txn.control)?;
    let ddl: Vec<_> = txn.ddl.iter().map(ddl_to_value).collect();
    let events: Result<Vec<_>> = txn.events.iter().map(cloud_event_to_value).collect();

    let value = Value::Record(vec![
        ("adapter".into(), Value::String(adapter)),
        ("position_at_json".into(), Value::String(at)),
        ("position_after_json".into(), Value::String(after)),
        ("part_number".into(), Value::Int(number)),
        ("part_last".into(), Value::Boolean(last)),
        ("ddl".into(), Value::Array(ddl)),
        ("control_json".into(), Value::String(control_json)),
        ("events".into(), Value::Array(events?)),
    ]);

    to_avro_datum(committed_transaction_v4_schema(), value)
        .map_err(|e| Error::Sink(format!("avro encode committed transaction: {e}")))
}

fn decode_record(schema: &Schema, bytes: &[u8], what: &str) -> Result<Vec<(String, Value)>> {
    let mut cursor = std::io::Cursor::new(bytes);
    let value = from_avro_datum(schema, &mut cursor, None)
        .map_err(|e| Error::Sink(format!("avro decode {what}: {e}")))?;
    match value {
        Value::Record(fields) => Ok(fields),
        _ => Err(Error::Sink(format!("avro decode {what}: expected record"))),
    }
}

fn decode_part(fields: &[(String, Value)]) -> Result<Option<TransactionPart>> {
    let number = match field(fields, "part_number") {
        Some(Value::Int(n)) => u32::try_from(*n)
            .map_err(|_| Error::Sink("avro decode: part_number is negative".into()))?,
        _ => return Err(Error::Sink("avro decode: missing part_number".into())),
    };
    let last = match field(fields, "part_last") {
        Some(Value::Boolean(b)) => *b,
        _ => return Err(Error::Sink("avro decode: missing part_last".into())),
    };
    Ok((number > 0).then_some(TransactionPart { number, last }))
}

/// Decode a `committed-transaction-v4` datum, what [`encode_committed_transaction`] writes.
pub fn decode_committed_transaction_v4(bytes: &[u8]) -> Result<CommittedTransaction> {
    let fields = decode_record(
        committed_transaction_v4_schema(),
        bytes,
        "committed transaction v4",
    )?;
    let adapter = get_string_field(&fields, "adapter")?;
    let position = if adapter.is_empty() {
        None
    } else {
        Some(SourcePosition {
            adapter,
            at: get_json_field(&fields, "position_at_json")?,
            after: get_json_field(&fields, "position_after_json")?,
        })
    };
    Ok(CommittedTransaction {
        position,
        ddl: decode_ddl_array(&fields)?,
        control: serde_json::from_str(&get_string_field(&fields, "control_json")?)
            .map_err(|e| Error::Sink(format!("avro decode control_json: {e}")))?,
        events: decode_events_array(&fields)?,
        part: decode_part(&fields)?,
    })
}

/// The position a v1 to v3 datum means. MySQL's was in its GTID fields.
fn position_before_v4(
    adapter: String,
    at: serde_json::Value,
    after: serde_json::Value,
    gtid: String,
    gtid_set_after: String,
) -> SourcePosition {
    if adapter != "mysql" {
        return SourcePosition { adapter, at, after };
    }
    let after = if gtid_set_after.is_empty() {
        serde_json::json!({})
    } else {
        serde_json::json!({ "gtid": gtid_set_after })
    };
    SourcePosition {
        adapter,
        at: serde_json::json!({ "gtid": gtid }),
        after,
    }
}

/// Decode a v3 datum: a part of a transaction delivered in parts. Read, not written.
pub fn decode_committed_transaction_v3(bytes: &[u8]) -> Result<CommittedTransaction> {
    let fields = decode_record(
        committed_transaction_v3_schema(),
        bytes,
        "committed transaction v3",
    )?;
    let position = position_before_v4(
        get_string_field(&fields, "adapter")?,
        get_json_field(&fields, "position_at_json")?,
        get_json_field(&fields, "position_after_json")?,
        get_string_field(&fields, "gtid")?,
        get_string_field(&fields, "gtid_set_after")?,
    );
    Ok(CommittedTransaction {
        position: Some(position),
        ddl: decode_ddl_array(&fields)?,
        control: serde_json::from_str(&get_string_field(&fields, "control_json")?)
            .map_err(|e| Error::Sink(format!("avro decode control_json: {e}")))?,
        events: decode_events_array(&fields)?,
        part: decode_part(&fields)?,
    })
}

/// Decode a v2 datum. Read, not written; v1 bytes are not accepted here.
pub fn decode_committed_transaction_v2(bytes: &[u8]) -> Result<CommittedTransaction> {
    let fields = decode_record(
        committed_transaction_v2_schema(),
        bytes,
        "committed transaction v2",
    )?;
    let position = position_before_v4(
        get_string_field(&fields, "adapter")?,
        get_json_field(&fields, "position_at_json")?,
        get_json_field(&fields, "position_after_json")?,
        get_string_field(&fields, "gtid")?,
        get_string_field(&fields, "gtid_set_after")?,
    );
    Ok(CommittedTransaction {
        part: None,
        position: Some(position),
        ddl: decode_ddl_array(&fields)?,
        control: serde_json::from_str(&get_string_field(&fields, "control_json")?)
            .map_err(|e| Error::Sink(format!("avro decode control_json: {e}")))?,
        events: decode_events_array(&fields)?,
    })
}

/// Decode a v1 datum: a whole MySQL commit. Read, not written.
pub fn decode_committed_transaction(bytes: &[u8]) -> Result<CommittedTransaction> {
    let fields = decode_record(
        committed_transaction_schema(),
        bytes,
        "committed transaction",
    )?;
    let position = position_before_v4(
        "mysql".into(),
        serde_json::json!({}),
        serde_json::json!({}),
        get_string_field(&fields, "gtid")?,
        get_string_field(&fields, "gtid_set_after")?,
    );
    Ok(CommittedTransaction {
        part: None,
        position: Some(position),
        ddl: decode_ddl_array(&fields)?,
        control: Vec::new(),
        events: decode_events_array(&fields)?,
    })
}

/// Encode a [`Delivery::Progress`] position as a `progress-v1` datum.
pub fn encode_progress(position: &SourcePosition) -> Result<Vec<u8>> {
    let value = Value::Record(vec![
        ("adapter".into(), Value::String(position.adapter.clone())),
        (
            "position_at_json".into(),
            Value::String(serde_json::to_string(&position.at)?),
        ),
        (
            "position_after_json".into(),
            Value::String(serde_json::to_string(&position.after)?),
        ),
    ]);
    to_avro_datum(progress_schema(), value)
        .map_err(|e| Error::Sink(format!("avro encode progress: {e}")))
}

/// Decode a `progress-v1` datum.
pub fn decode_progress(bytes: &[u8]) -> Result<SourcePosition> {
    let fields = decode_record(progress_schema(), bytes, "progress")?;
    Ok(SourcePosition {
        adapter: get_string_field(&fields, "adapter")?,
        at: get_json_field(&fields, "position_at_json")?,
        after: get_json_field(&fields, "position_after_json")?,
    })
}

fn nullable_value(value: Option<Value>) -> Value {
    match value {
        Some(inner) => Value::Union(1, Box::new(inner)),
        None => Value::Union(0, Box::new(Value::Null)),
    }
}

fn nullable_field<'a>(fields: &'a [(String, Value)], name: &str) -> Option<&'a Value> {
    match field(fields, name) {
        Some(Value::Union(_, inner)) if !matches!(**inner, Value::Null) => Some(inner),
        _ => None,
    }
}

/// Encode a [`Delivery::Schema`] definition as a `table-schema-v1` datum.
pub fn encode_table_schema(schema: &TableSchema) -> Result<Vec<u8>> {
    let columns: Vec<_> = schema
        .columns
        .iter()
        .map(|column| {
            Value::Record(vec![
                ("name".into(), Value::String(column.name.clone())),
                ("host_type".into(), Value::String(column.host_type.clone())),
                (
                    "nullable".into(),
                    nullable_value(column.nullable.map(Value::Boolean)),
                ),
                ("key".into(), Value::Boolean(column.key)),
            ])
        })
        .collect();
    let value = Value::Record(vec![
        (
            "database".into(),
            Value::String(schema.subject.database.clone()),
        ),
        ("table".into(), Value::String(schema.subject.table.clone())),
        (
            "identity".into(),
            nullable_value(schema.identity.clone().map(Value::String)),
        ),
        ("columns".into(), Value::Array(columns)),
    ]);
    to_avro_datum(table_schema_schema(), value)
        .map_err(|e| Error::Sink(format!("avro encode table schema: {e}")))
}

/// Decode a `table-schema-v1` datum.
pub fn decode_table_schema(bytes: &[u8]) -> Result<TableSchema> {
    let fields = decode_record(table_schema_schema(), bytes, "table schema")?;
    let Some(Value::Array(items)) = field(&fields, "columns") else {
        return Err(Error::Sink("avro decode: columns must be array".into()));
    };
    let columns: Result<Vec<_>> = items
        .iter()
        .map(|item| {
            let Value::Record(record) = item else {
                return Err(Error::Sink(
                    "avro decode: columns item must be record".into(),
                ));
            };
            let nullable = match nullable_field(record, "nullable") {
                Some(Value::Boolean(b)) => Some(*b),
                _ => None,
            };
            let key = matches!(field(record, "key"), Some(Value::Boolean(true)));
            Ok(ColumnSchema {
                name: get_string_field(record, "name")?,
                host_type: get_string_field(record, "host_type")?,
                nullable,
                key,
            })
        })
        .collect();
    let identity = match nullable_field(&fields, "identity") {
        Some(Value::String(s)) => Some(s.clone()),
        _ => None,
    };
    Ok(TableSchema {
        subject: TableRef::new(
            get_string_field(&fields, "database")?,
            get_string_field(&fields, "table")?,
        ),
        identity,
        columns: columns?,
    })
}

/// Encode any delivery, with its schema id and HTTP Content-Type.
pub fn encode_delivery(delivery: &Delivery) -> Result<(Vec<u8>, &'static str, &'static str)> {
    Ok(match delivery {
        Delivery::Commit(txn) => (
            encode_committed_transaction(txn)?,
            COMMITTED_TRANSACTION_V4_SCHEMA_ID,
            CONTENT_TYPE_COMMITTED_TXN_AVRO,
        ),
        Delivery::Progress { position } => (
            encode_progress(position)?,
            PROGRESS_SCHEMA_ID,
            CONTENT_TYPE_PROGRESS_AVRO,
        ),
        Delivery::Schema(schema) => (
            encode_table_schema(schema)?,
            TABLE_SCHEMA_SCHEMA_ID,
            CONTENT_TYPE_TABLE_SCHEMA_AVRO,
        ),
    })
}

fn decode_ddl_array(fields: &[(String, Value)]) -> Result<Vec<DdlStatement>> {
    let Some(Value::Array(items)) = field(fields, "ddl") else {
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
    let Some(Value::Array(items)) = field(fields, "events") else {
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
    use crate::event::ChangeOp;
    use crate::transaction::{ControlEvent, ControlKind};

    fn sample_row_event(n: i32) -> CloudEvent {
        CloudEvent::row_change(
            "mysql://lab/ce-stream",
            &TableRef::new("db", "t1"),
            ChangeOp::Insert,
            serde_json::json!({"op": "insert", "after": {"id": n}}),
            serde_json::Map::new(),
        )
    }

    fn mysql_position(gtid: &str, set_after: &str) -> SourcePosition {
        SourcePosition {
            adapter: "mysql".into(),
            at: serde_json::json!({ "gtid": gtid }),
            after: serde_json::json!({ "gtid": set_after }),
        }
    }

    fn sample_committed_txn() -> CommittedTransaction {
        CommittedTransaction {
            part: None,
            position: Some(mysql_position(
                "36682cf3-a048-11ed-b4b3-0242ac110004:42",
                "36682cf3-a048-11ed-b4b3-0242ac110004:1-42",
            )),
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
    fn every_adapter_writes_v4() {
        let txn = sample_committed_txn();
        assert_eq!(
            committed_transaction_schema_id(&txn),
            COMMITTED_TRANSACTION_V4_SCHEMA_ID
        );
        let bytes = encode_committed_transaction(&txn).unwrap();
        let back = decode_committed_transaction_v4(&bytes).unwrap();
        assert_eq!(back.position, txn.position);
        assert_eq!(back.part, None);
        assert_eq!(back.ddl, txn.ddl);
        assert_eq!(back.events.len(), 3);
        assert_eq!(back.events[0].subject, "db.t1");
    }

    #[test]
    fn a_part_roundtrips_v4() {
        let mut part = sample_committed_txn();
        part.position.as_mut().unwrap().after = serde_json::json!({});
        part.part = Some(TransactionPart {
            number: 2,
            last: false,
        });
        let bytes = encode_committed_transaction(&part).unwrap();
        let back = decode_committed_transaction_v4(&bytes).unwrap();
        assert_eq!(back.part, part.part);
        assert_eq!(back.position, part.position);
        assert!(!back.ends_transaction());
    }

    #[test]
    fn a_mongo_commit_keeps_its_control_events_in_v4() {
        let txn = CommittedTransaction {
            part: None,
            position: Some(SourcePosition {
                adapter: "mongo".into(),
                at: serde_json::json!({"resume_token": {"_data": "a"}}),
                after: serde_json::json!({"resume_token": {"_data": "b"}}),
            }),
            control: vec![ControlEvent {
                kind: ControlKind::Dropped,
                subject: TableRef::new("app", "orders"),
                identity: None,
            }],
            events: vec![sample_row_event(1)],
            ..Default::default()
        };
        let bytes = encode_committed_transaction(&txn).unwrap();
        let back = decode_committed_transaction_v4(&bytes).unwrap();
        assert_eq!(back.position.unwrap().after["resume_token"]["_data"], "b");
        assert_eq!(back.control[0].kind, ControlKind::Dropped);
        assert_eq!(back.events.len(), 1);
    }

    fn v0_5_fields(gtid: &str, set_after: &str) -> Vec<(String, Value)> {
        vec![
            ("gtid".into(), Value::String(gtid.into())),
            ("gtid_set_after".into(), Value::String(set_after.into())),
        ]
    }

    fn v0_5_rest() -> Vec<(String, Value)> {
        vec![
            ("ddl".into(), Value::Array(vec![])),
            (
                "events".into(),
                Value::Array(vec![cloud_event_to_value(&sample_row_event(1)).unwrap()]),
            ),
        ]
    }

    #[test]
    fn a_v1_commit_is_read_with_its_position_from_the_gtid_fields() {
        let mut fields = v0_5_fields("sid:5", "sid:1-5");
        fields.extend(v0_5_rest());
        let bytes = to_avro_datum(committed_transaction_schema(), Value::Record(fields)).unwrap();
        let back = decode_committed_transaction(&bytes).unwrap();
        assert_eq!(back.position, Some(mysql_position("sid:5", "sid:1-5")));
        assert_eq!(back.events.len(), 1);
    }

    #[test]
    fn a_v3_mysql_part_is_read_with_nothing_after_it() {
        let mut fields = vec![
            ("adapter".to_string(), Value::String("mysql".into())),
            ("position_at_json".into(), Value::String("{}".into())),
            ("position_after_json".into(), Value::String("{}".into())),
        ];
        fields.extend(v0_5_fields("sid:5", ""));
        fields.push(("part_number".into(), Value::Int(1)));
        fields.push(("part_last".into(), Value::Boolean(false)));
        fields.push(("ddl".into(), Value::Array(vec![])));
        fields.push(("control_json".into(), Value::String("[]".into())));
        fields.push(("events".into(), Value::Array(vec![])));
        let bytes =
            to_avro_datum(committed_transaction_v3_schema(), Value::Record(fields)).unwrap();
        let back = decode_committed_transaction_v3(&bytes).unwrap();
        let position = back.position.unwrap();
        assert_eq!(position.at["gtid"], "sid:5");
        assert_eq!(position.after, serde_json::json!({}));
        assert_eq!(
            back.part,
            Some(TransactionPart {
                number: 1,
                last: false
            })
        );
    }

    #[test]
    fn a_v2_mongo_commit_is_read_as_it_was() {
        let mut fields = vec![
            ("adapter".to_string(), Value::String("mongo".into())),
            (
                "position_at_json".into(),
                Value::String(r#"{"resume_token":{"_data":"a"}}"#.into()),
            ),
            (
                "position_after_json".into(),
                Value::String(r#"{"resume_token":{"_data":"b"}}"#.into()),
            ),
        ];
        fields.extend(v0_5_fields("", ""));
        fields.push(("ddl".into(), Value::Array(vec![])));
        fields.push((
            "control_json".into(),
            Value::String(
                r#"[{"kind":"renamed","subject":{"database":"a","table":"t"},"renamed_to":{"database":"a","table":"u"}}]"#
                    .into(),
            ),
        ));
        fields.push(("events".into(), Value::Array(vec![])));
        let bytes =
            to_avro_datum(committed_transaction_v2_schema(), Value::Record(fields)).unwrap();
        let back = decode_committed_transaction_v2(&bytes).unwrap();
        assert_eq!(back.position.unwrap().after["resume_token"]["_data"], "b");
        assert_eq!(
            back.control[0].kind,
            ControlKind::Renamed {
                to: TableRef::new("a", "u")
            }
        );
    }

    #[test]
    fn progress_and_table_schema_roundtrip() {
        let position = mysql_position("sid:5", "sid:1-5");
        assert_eq!(
            decode_progress(&encode_progress(&position).unwrap()).unwrap(),
            position
        );
        let schema = TableSchema {
            subject: TableRef::new("public", "orders"),
            identity: Some("16420".into()),
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
                    host_type: "integer".into(),
                    nullable: Some(false),
                    key: true,
                },
                ColumnSchema {
                    name: "note".into(),
                    host_type: "text".into(),
                    nullable: None,
                    key: false,
                },
            ],
        };
        assert_eq!(
            decode_table_schema(&encode_table_schema(&schema).unwrap()).unwrap(),
            schema
        );
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
        assert_eq!(back.position, txn.position);
        assert_eq!(back.ddl.len(), 1);
        assert_eq!(back.events.len(), 3);
    }
}

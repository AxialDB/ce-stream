use serde::{Deserialize, Serialize};

/// Database + table (or collection) identity (adapter-agnostic).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableRef {
    pub database: String,
    pub table: String,
}

impl TableRef {
    pub fn new(database: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            database: database.into(),
            table: table.into(),
        }
    }

    pub fn as_subject(&self) -> String {
        format!("{}.{}", self.database, self.table)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChangeOp {
    Insert,
    Update,
    Delete,
}

impl ChangeOp {
    pub fn as_ce_type(&self) -> &'static str {
        match self {
            Self::Insert => "io.ce-stream.row.inserted",
            Self::Update => "io.ce-stream.row.updated",
            Self::Delete => "io.ce-stream.row.deleted",
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

/// How much row payload to put in CloudEvent `data`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PayloadMode {
    /// Full before/after images (default).
    #[default]
    Full,
    /// Op + table only; smaller / lower impact for light consumers.
    Signal,
}

/// Wire encoding for sinks (JSON remains the default interchange).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SinkFormat {
    /// CloudEvents structured-mode JSON (`application/cloudevents+json`).
    #[default]
    Json,
    /// Single Avro datum (`application/cloudevents+avro`); see `avro_encode`.
    Avro,
}

/// CloudEvents 1.0 structured-mode JSON (subset we emit).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudEvent {
    pub specversion: String,
    pub id: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub source: String,
    pub subject: String,
    pub time: String,
    /// Adapter-specific extension attrs (gtid, file, pos, ...).
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub extensions: serde_json::Map<String, serde_json::Value>,
    pub data: serde_json::Value,
}

impl CloudEvent {
    pub fn row_change(
        source: impl Into<String>,
        table: &TableRef,
        op: ChangeOp,
        data: serde_json::Value,
        extensions: serde_json::Map<String, serde_json::Value>,
    ) -> Self {
        Self {
            specversion: "1.0".into(),
            id: uuid::Uuid::new_v4().to_string(),
            ty: op.as_ce_type().into(),
            source: source.into(),
            subject: table.as_subject(),
            time: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            extensions,
            data,
        }
    }

    /// CloudEvents structured-mode JSON: core attrs + extensions at top level, then `data`.
    pub fn to_structured_json(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        map.insert("specversion".into(), self.specversion.clone().into());
        map.insert("id".into(), self.id.clone().into());
        map.insert("type".into(), self.ty.clone().into());
        map.insert("source".into(), self.source.clone().into());
        map.insert("subject".into(), self.subject.clone().into());
        map.insert("time".into(), self.time.clone().into());
        for (k, v) in &self.extensions {
            map.insert(k.clone(), v.clone());
        }
        map.insert("data".into(), self.data.clone());
        serde_json::Value::Object(map)
    }
}

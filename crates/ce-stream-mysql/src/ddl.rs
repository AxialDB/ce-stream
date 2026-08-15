//! DDL classification and CloudEvent mapping (`io.ce-stream.mysql.ddl`).

use ce_stream_core::transaction::DdlStatement;
use ce_stream_core::CloudEvent;

pub const DDL_CE_TYPE: &str = "io.ce-stream.mysql.ddl";

/// True when the query text is schema-affecting DDL (case-insensitive prefix).
pub fn is_ddl_query(query: &str) -> bool {
    let q = query.trim_start();
    let upper = q.to_ascii_uppercase();
    upper.starts_with("CREATE")
        || upper.starts_with("ALTER")
        || upper.starts_with("DROP")
        || upper.starts_with("TRUNCATE")
        || upper.starts_with("RENAME")
}

/// Transaction control statements that carry no delivery payload.
pub fn is_ignored_transaction_query(query: &str) -> bool {
    let q = query
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_ascii_uppercase();
    matches!(q.as_str(), "BEGIN" | "COMMIT" | "ROLLBACK")
}

/// Build a DDL CloudEvent for row-mode delivery (same extensions as row events).
pub fn ddl_cloud_event(
    source_id: &str,
    stmt: &DdlStatement,
    gtid: &str,
    gtid_set_after: &str,
) -> CloudEvent {
    let mut extensions = serde_json::Map::new();
    extensions.insert("gtid".into(), serde_json::Value::String(gtid.to_string()));
    if !gtid_set_after.is_empty() {
        extensions.insert(
            "gtidset".into(),
            serde_json::Value::String(gtid_set_after.to_string()),
        );
    }

    CloudEvent {
        specversion: "1.0".into(),
        id: uuid::Uuid::new_v4().to_string(),
        ty: DDL_CE_TYPE.into(),
        source: source_id.to_string(),
        subject: stmt.schema.clone(),
        time: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        extensions,
        data: serde_json::json!({
            "schema": stmt.schema,
            "query": stmt.query,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_ddl_prefixes() {
        assert!(is_ddl_query("CREATE TABLE t (id INT)"));
        assert!(is_ddl_query("  alter table t add column x int"));
        assert!(is_ddl_query("DROP TABLE t"));
        assert!(is_ddl_query("TRUNCATE db.t"));
        assert!(is_ddl_query("RENAME TABLE a TO b"));
        assert!(!is_ddl_query("INSERT INTO t VALUES (1)"));
        assert!(!is_ddl_query("BEGIN"));
    }

    #[test]
    fn ignores_txn_control_queries() {
        assert!(is_ignored_transaction_query("BEGIN"));
        assert!(is_ignored_transaction_query("commit;"));
        assert!(is_ignored_transaction_query("  ROLLBACK  "));
        assert!(!is_ignored_transaction_query("CREATE TABLE t (id INT)"));
    }

    #[test]
    fn ddl_cloud_event_shape() {
        let ev = ddl_cloud_event(
            "mysql://test",
            &DdlStatement {
                schema: "app".into(),
                query: "ALTER TABLE t ADD COLUMN x INT".into(),
            },
            "uuid:1",
            "uuid:1-1",
        );
        assert_eq!(ev.ty, DDL_CE_TYPE);
        assert_eq!(ev.subject, "app");
        assert_eq!(ev.data["schema"], "app");
        assert_eq!(ev.data["query"], "ALTER TABLE t ADD COLUMN x INT");
        assert_eq!(
            ev.extensions.get("gtid").and_then(|v| v.as_str()),
            Some("uuid:1")
        );
    }
}

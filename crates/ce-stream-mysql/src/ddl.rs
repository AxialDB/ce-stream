//! DDL classification and CloudEvent mapping (`io.ce-stream.mysql.ddl`).

use ce_stream_core::transaction::DdlStatement;
use ce_stream_core::CloudEvent;

pub const DDL_CE_TYPE: &str = "io.ce-stream.mysql.ddl";

/// The statement from its first word on. A client may send a comment in
/// front (`/* ApplicationName=... */ ALTER TABLE ...`), and the server logs
/// the statement with it. What is inside `/*!50003 ... */` is not a comment
/// to MySQL: it is the statement.
pub fn statement_start(query: &str) -> &str {
    let mut rest = query;
    loop {
        let text = rest.trim_start();
        if let Some(inner) = text.strip_prefix("/*!") {
            rest = inner.trim_start_matches(|c: char| c.is_ascii_digit());
        } else if let Some(comment) = text.strip_prefix("/*") {
            match comment.find("*/") {
                Some(end) => rest = &comment[end + 2..],
                None => return "",
            }
        } else if text.starts_with('#') || text.starts_with("-- ") || text.starts_with("--\t") {
            match text.find('\n') {
                Some(end) => rest = &text[end + 1..],
                None => return "",
            }
        } else {
            return text;
        }
    }
}

/// True when the query text is schema-affecting DDL (case-insensitive prefix).
pub fn is_ddl_query(query: &str) -> bool {
    let upper = statement_start(query).to_ascii_uppercase();
    upper.starts_with("CREATE")
        || upper.starts_with("ALTER")
        || upper.starts_with("DROP")
        || upper.starts_with("TRUNCATE")
        || upper.starts_with("RENAME")
}

/// What a `Query` event means for the boundaries of its transaction.
///
/// The server writes a transaction in one of two shapes. A transaction in
/// brackets starts with `BEGIN` and ends with an `Xid` event (InnoDB) or a
/// `COMMIT` query (an engine without transactions). Any other statement is a
/// transaction of one `Query` event with nothing after it: a DDL statement,
/// `GRANT`, `ANALYZE TABLE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryKind {
    /// Opens a transaction whose end comes later: `BEGIN`, `XA START`.
    Begin,
    /// Ends the open transaction: `COMMIT`.
    Commit,
    /// `ROLLBACK`.
    Rollback,
    /// A DDL statement.
    Ddl,
    /// Any other statement.
    Other,
}

pub fn query_kind(query: &str) -> QueryKind {
    let text = statement_start(query)
        .trim_end()
        .trim_end_matches(';')
        .trim_end()
        .to_ascii_uppercase();
    match text.as_str() {
        "BEGIN" | "START TRANSACTION" => QueryKind::Begin,
        "COMMIT" => QueryKind::Commit,
        "ROLLBACK" => QueryKind::Rollback,
        _ if text.starts_with("XA START") || text.starts_with("XA BEGIN") => QueryKind::Begin,
        _ if is_ddl_query(&text) => QueryKind::Ddl,
        _ => QueryKind::Other,
    }
}

/// `CREATE TABLE ... SELECT` is logged as `CREATE TABLE ... START
/// TRANSACTION`, followed by the rows and the commit: the statement opens a
/// transaction instead of being one.
pub fn ddl_opens_transaction(query: &str) -> bool {
    let text = query.trim_end().trim_end_matches(';').trim_end();
    let upper = text.to_ascii_uppercase();
    let mut words = upper.split_whitespace().rev();
    words.next() == Some("TRANSACTION") && words.next() == Some("START")
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
    fn a_comment_in_front_does_not_hide_a_statement() {
        for query in [
            "/* ApplicationName=DBeaver 26.2.1 - SQLEditor <Script-9.sql> */ TRUNCATE TABLE t",
            "-- nightly\nALTER TABLE t DROP COLUMN x",
            "# note\n  /* two */ /* three */ DROP TABLE t",
            "/*!40000 ALTER TABLE `t` DISABLE KEYS */",
        ] {
            assert!(is_ddl_query(query), "{query}");
            assert_eq!(query_kind(query), QueryKind::Ddl, "{query}");
        }
        assert!(!is_ddl_query(
            "/* CREATE TABLE t */ INSERT INTO t VALUES (1)"
        ));
        assert_eq!(statement_start("/* never closed"), "");
    }

    #[test]
    fn query_kinds() {
        assert_eq!(query_kind("BEGIN"), QueryKind::Begin);
        assert_eq!(query_kind("XA START X'61',X'',1"), QueryKind::Begin);
        assert_eq!(query_kind("COMMIT"), QueryKind::Commit);
        assert_eq!(query_kind("commit;"), QueryKind::Commit);
        assert_eq!(query_kind("ROLLBACK"), QueryKind::Rollback);
        assert_eq!(query_kind("ROLLBACK TO `sp`"), QueryKind::Other);
        assert_eq!(query_kind("SAVEPOINT `sp`"), QueryKind::Other);
        assert_eq!(query_kind("XA END X'61',X'',1"), QueryKind::Other);
        assert_eq!(query_kind("XA COMMIT X'61',X'',1"), QueryKind::Other);
        assert_eq!(
            query_kind("GRANT SELECT ON `app`.* TO 'u'@'localhost'"),
            QueryKind::Other
        );
        assert_eq!(query_kind("ANALYZE TABLE app.t"), QueryKind::Other);
        assert_eq!(query_kind("DROP DATABASE app"), QueryKind::Ddl);
    }

    #[test]
    fn create_table_select_opens_a_transaction() {
        assert!(ddl_opens_transaction(
            "CREATE TABLE `app`.`copy` (\n  `id` int NOT NULL\n) START TRANSACTION"
        ));
        assert!(!ddl_opens_transaction("CREATE TABLE app.t (id INT)"));
        assert!(!ddl_opens_transaction(
            "ALTER TABLE t COMMENT 'start transaction'"
        ));
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

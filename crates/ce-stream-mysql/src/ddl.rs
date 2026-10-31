//! DDL classification and CloudEvent mapping (`io.ce-stream.mysql.ddl`).

use ce_stream_core::event::TableRef;
use ce_stream_core::transaction::{ControlEvent, ControlKind, DdlStatement};
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

/// One word or quoted name of a statement, and what follows it.
fn next_token(text: &str) -> Option<(String, &str)> {
    let text = text.trim_start();
    let first = text.chars().next()?;
    if first == '`' {
        let mut name = String::new();
        let mut rest = &text[1..];
        loop {
            let end = rest.find('`')?;
            name.push_str(&rest[..end]);
            rest = &rest[end + 1..];
            // A doubled backquote is a backquote in the name.
            match rest.strip_prefix('`') {
                Some(after) => {
                    name.push('`');
                    rest = after;
                }
                None => return Some((name, rest)),
            }
        }
    }
    if ",.;()=".contains(first) {
        return Some((first.to_string(), &text[first.len_utf8()..]));
    }
    let end = text
        .find(|c: char| c.is_whitespace() || ",.;()=`".contains(c))
        .unwrap_or(text.len());
    Some((text[..end].to_string(), &text[end..]))
}

fn next_is<'a>(text: &'a str, word: &str) -> Option<&'a str> {
    let (token, rest) = next_token(text)?;
    token.eq_ignore_ascii_case(word).then_some(rest)
}

fn skip_if_exists(text: &str) -> &str {
    next_is(text, "IF")
        .and_then(|rest| next_is(rest, "EXISTS"))
        .unwrap_or(text)
}

/// `db.t` or `t`, in the statement's default database.
fn next_table<'a>(schema: &str, text: &'a str) -> Option<(TableRef, &'a str)> {
    let (first, rest) = next_token(text)?;
    if let Some(after_dot) = rest.strip_prefix('.') {
        let (table, rest) = next_token(after_dot)?;
        return Some((TableRef::new(first, table), rest));
    }
    Some((TableRef::new(schema, first), rest))
}

fn control(kind: ControlKind, subject: TableRef) -> ControlEvent {
    ControlEvent {
        kind,
        subject,
        identity: None,
    }
}

/// The typed controls a DDL statement means, beside its text. A statement this
/// does not read gives none: the text is there for the consumer.
pub fn ddl_controls(schema: &str, query: &str) -> Vec<ControlEvent> {
    read_controls(schema, statement_start(query)).unwrap_or_default()
}

fn read_controls(schema: &str, text: &str) -> Option<Vec<ControlEvent>> {
    let mut out = Vec::new();
    let (verb, rest) = next_token(text)?;
    match verb.to_ascii_uppercase().as_str() {
        "DROP" => {
            let (what, rest) = next_token(rest)?;
            match what.to_ascii_uppercase().as_str() {
                "TABLE" => {
                    let mut rest = skip_if_exists(rest);
                    loop {
                        let (table, after) = next_table(schema, rest)?;
                        out.push(control(ControlKind::Dropped, table));
                        match after.trim_start().strip_prefix(',') {
                            Some(more) => rest = more,
                            None => break,
                        }
                    }
                }
                "DATABASE" | "SCHEMA" => {
                    let (name, _) = next_token(skip_if_exists(rest))?;
                    out.push(control(
                        ControlKind::DatabaseDropped,
                        TableRef::new(name, ""),
                    ));
                }
                _ => {}
            }
        }
        "RENAME" => {
            let mut rest = next_is(rest, "TABLE").or_else(|| next_is(rest, "TABLES"))?;
            loop {
                let (from, after) = next_table(schema, rest)?;
                let after = next_is(after, "TO")?;
                let (to, after) = next_table(schema, after)?;
                out.push(control(ControlKind::Renamed { to }, from));
                match after.trim_start().strip_prefix(',') {
                    Some(more) => rest = more,
                    None => break,
                }
            }
        }
        "TRUNCATE" => {
            let rest = next_is(rest, "TABLE").unwrap_or(rest);
            let (table, _) = next_table(schema, rest)?;
            out.push(control(ControlKind::Truncated, table));
        }
        "ALTER" => {
            let rest = next_is(rest, "TABLE")?;
            let (table, mut words) = next_table(schema, rest)?;
            let mut renamed = None;
            let mut other = false;
            let mut depth = 0i32;
            let mut at_clause = true;
            while let Some((token, after)) = next_token(words) {
                words = after;
                match token.as_str() {
                    "(" => depth += 1,
                    ")" => depth -= 1,
                    "," if depth == 0 => {
                        at_clause = true;
                        continue;
                    }
                    ";" => break,
                    _ if at_clause && depth == 0 => {
                        if token.eq_ignore_ascii_case("RENAME") {
                            let target = next_is(words, "TO")
                                .or_else(|| next_is(words, "AS"))
                                .unwrap_or(words);
                            let (word, _) = next_token(target)?;
                            let inside = ["COLUMN", "INDEX", "KEY"]
                                .iter()
                                .any(|w| word.eq_ignore_ascii_case(w));
                            if inside {
                                other = true;
                            } else {
                                let (to, after) = next_table(schema, target)?;
                                renamed = Some(to);
                                words = after;
                            }
                        } else {
                            other = true;
                        }
                    }
                    _ => {}
                }
                at_clause = false;
            }
            if other || renamed.is_none() {
                out.push(control(ControlKind::DefinitionChanged, table.clone()));
            }
            if let Some(to) = renamed {
                out.push(control(ControlKind::Renamed { to }, table));
            }
        }
        _ => {}
    }
    Some(out)
}

/// An XA statement as the binlog holds it. The id is the text after the verb,
/// `X'74776f',X'',1`, the same in each of a transaction's statements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum XaStatement {
    Start(String),
    Commit(String),
    Rollback(String),
}

pub fn xa_statement(query: &str) -> Option<XaStatement> {
    let text = statement_start(query).trim_end().trim_end_matches(';');
    let id_after = |verb: &str| -> Option<String> {
        let head = text.get(..verb.len())?;
        head.eq_ignore_ascii_case(verb)
            .then(|| text[verb.len()..].trim().to_string())
    };
    if let Some(id) = id_after("XA START ").or_else(|| id_after("XA BEGIN ")) {
        Some(XaStatement::Start(id))
    } else if let Some(id) = id_after("XA COMMIT ") {
        Some(XaStatement::Commit(id))
    } else {
        id_after("XA ROLLBACK ").map(XaStatement::Rollback)
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

    fn kinds(query: &str) -> Vec<(ControlKind, String)> {
        ddl_controls("app", query)
            .into_iter()
            .map(|c| (c.kind, c.subject.as_subject()))
            .collect()
    }

    #[test]
    fn a_drop_names_each_table_in_its_default_database() {
        assert_eq!(
            kinds("DROP TABLE IF EXISTS `t1`, other.`t 2` /* generated by server */"),
            [
                (ControlKind::Dropped, "app.t1".to_string()),
                (ControlKind::Dropped, "other.t 2".to_string()),
            ]
        );
    }

    #[test]
    fn a_rename_says_where_each_table_went() {
        assert_eq!(
            kinds("RENAME TABLE a TO b, x.c TO y.d"),
            [
                (
                    ControlKind::Renamed {
                        to: TableRef::new("app", "b")
                    },
                    "app.a".to_string()
                ),
                (
                    ControlKind::Renamed {
                        to: TableRef::new("y", "d")
                    },
                    "x.c".to_string()
                ),
            ]
        );
    }

    #[test]
    fn truncate_and_drop_database() {
        assert_eq!(
            kinds("TRUNCATE TABLE `t`"),
            [(ControlKind::Truncated, "app.t".to_string())]
        );
        assert_eq!(
            kinds("TRUNCATE t"),
            [(ControlKind::Truncated, "app.t".to_string())]
        );
        let dropped = ddl_controls("app", "DROP DATABASE IF EXISTS `old`");
        assert_eq!(dropped[0].kind, ControlKind::DatabaseDropped);
        assert_eq!(dropped[0].subject, TableRef::new("old", ""));
    }

    #[test]
    fn an_alter_changes_the_definition_and_may_rename() {
        assert_eq!(
            kinds("ALTER TABLE t ADD COLUMN x INT, DROP COLUMN y"),
            [(ControlKind::DefinitionChanged, "app.t".to_string())]
        );
        assert_eq!(
            kinds("ALTER TABLE t RENAME TO u"),
            [(
                ControlKind::Renamed {
                    to: TableRef::new("app", "u")
                },
                "app.t".to_string()
            )]
        );
        let both = kinds("ALTER TABLE t ADD COLUMN x INT, RENAME AS z.u");
        assert_eq!(both[0].0, ControlKind::DefinitionChanged);
        assert_eq!(
            both[1].0,
            ControlKind::Renamed {
                to: TableRef::new("z", "u")
            }
        );
        // These rename something inside the table.
        assert_eq!(
            kinds("ALTER TABLE t RENAME COLUMN a TO b"),
            [(ControlKind::DefinitionChanged, "app.t".to_string())]
        );
        assert_eq!(
            kinds("ALTER TABLE t RENAME INDEX a TO b"),
            [(ControlKind::DefinitionChanged, "app.t".to_string())]
        );
    }

    #[test]
    fn what_it_cannot_read_gives_no_control() {
        assert!(kinds("CREATE TABLE t (id INT)").is_empty());
        assert!(kinds("DROP INDEX i ON t").is_empty());
        assert!(kinds("RENAME USER a TO b").is_empty());
        assert!(kinds("").is_empty());
    }

    #[test]
    fn xa_statements_carry_their_id() {
        assert_eq!(
            xa_statement("XA START X'61',X'',1"),
            Some(XaStatement::Start("X'61',X'',1".into()))
        );
        assert_eq!(
            xa_statement("xa commit X'61',X'',1;"),
            Some(XaStatement::Commit("X'61',X'',1".into()))
        );
        assert_eq!(
            xa_statement("XA ROLLBACK X'61',X'',1"),
            Some(XaStatement::Rollback("X'61',X'',1".into()))
        );
        assert_eq!(xa_statement("XA END X'61',X'',1"), None);
        assert_eq!(xa_statement("SELECT 1"), None);
    }

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

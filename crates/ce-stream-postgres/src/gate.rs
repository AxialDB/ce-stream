//! What the stream needs of the server and the included tables.
//! What the embedder wants on top of that is the embedder's to check.

use ce_stream_core::error::Result;
use ce_stream_core::event::TableRef;
use ce_stream_core::gate::GateReport;
use serde::Deserialize;

use crate::options::{PostgresOptions, PostgresStreamOptions};
use crate::wire::{quote_literal, Connection};

const SERVER_SQL: &str = "SELECT json_build_object(
    'version_num', current_setting('server_version_num')::int,
    'wal_level', current_setting('wal_level'),
    'in_recovery', pg_is_in_recovery(),
    'max_replication_slots', current_setting('max_replication_slots')::int,
    'slots_used', (SELECT count(*) FROM pg_replication_slots),
    'max_wal_senders', current_setting('max_wal_senders')::int,
    'senders_used', (SELECT count(*) FROM pg_stat_replication),
    'max_slot_wal_keep_size', current_setting('max_slot_wal_keep_size'),
    'hot_standby_feedback', current_setting('hot_standby_feedback'),
    'idle_replication_slot_timeout', current_setting('idle_replication_slot_timeout', true),
    'encoding', (SELECT pg_encoding_to_char(encoding) FROM pg_database
                 WHERE datname = current_database()),
    'can_replicate', (SELECT rolreplication OR rolsuper FROM pg_roles
                      WHERE rolname = current_user))::text";

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ServerFacts {
    pub(crate) version_num: u32,
    pub(crate) wal_level: String,
    pub(crate) in_recovery: bool,
    pub(crate) max_replication_slots: i64,
    pub(crate) slots_used: i64,
    pub(crate) max_wal_senders: i64,
    pub(crate) senders_used: i64,
    pub(crate) max_slot_wal_keep_size: String,
    pub(crate) hot_standby_feedback: String,
    pub(crate) idle_replication_slot_timeout: Option<String>,
    pub(crate) encoding: String,
    pub(crate) can_replicate: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct PublicationFacts {
    pub(crate) insert: bool,
    pub(crate) update: bool,
    pub(crate) delete: bool,
    pub(crate) truncate: bool,
    pub(crate) via_root: bool,
    /// `s` on 18 when stored generated columns are published.
    pub(crate) generated: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RelationFacts {
    pub(crate) name: String,
    pub(crate) relkind: String,
    pub(crate) relpersistence: String,
    pub(crate) relreplident: String,
    pub(crate) has_primary_key: bool,
    pub(crate) generated_columns: i64,
    pub(crate) in_publication: bool,
    pub(crate) has_row_filter: bool,
    pub(crate) has_column_list: bool,
    /// For a partitioned parent: every leaf below it, with the same facts.
    #[serde(default)]
    pub(crate) partitions: Vec<RelationFacts>,
}

fn publication_sql(publication: &str) -> String {
    format!(
        "SELECT json_build_object(
            'insert', p.pubinsert, 'update', p.pubupdate, 'delete', p.pubdelete,
            'truncate', p.pubtruncate, 'via_root', p.pubviaroot,
            'generated', to_jsonb(p) ->> 'pubgencols')::text
         FROM pg_publication p WHERE p.pubname = {}",
        quote_literal(publication)
    )
}

/// The facts of one relation `c`, under the alias `n` for its namespace.
fn relation_facts_sql(publication: &str) -> String {
    let publication = quote_literal(publication);
    format!(
        "'name', n.nspname || '.' || c.relname,
         'relkind', c.relkind, 'relpersistence', c.relpersistence,
         'relreplident', c.relreplident,
         'has_primary_key', EXISTS (SELECT 1 FROM pg_index i
                                    WHERE i.indrelid = c.oid AND i.indisprimary),
         'generated_columns', (SELECT count(*) FROM pg_attribute a
                               WHERE a.attrelid = c.oid AND a.attnum > 0
                                 AND NOT a.attisdropped AND a.attgenerated <> ''),
         'in_publication', EXISTS (SELECT 1 FROM pg_publication_tables t
                                   WHERE t.pubname = {publication}
                                     AND t.schemaname = n.nspname AND t.tablename = c.relname),
         'has_row_filter', EXISTS (SELECT 1 FROM pg_publication_rel r
                                   JOIN pg_publication p ON p.oid = r.prpubid
                                   WHERE p.pubname = {publication} AND r.prqual IS NOT NULL
                                     AND (r.prrelid = c.oid OR r.prrelid IN
                                          (SELECT pg_partition_ancestors(c.oid)))),
         'has_column_list', EXISTS (SELECT 1 FROM pg_publication_rel r
                                    JOIN pg_publication p ON p.oid = r.prpubid
                                    WHERE p.pubname = {publication} AND r.prattrs IS NOT NULL
                                      AND (r.prrelid = c.oid OR r.prrelid IN
                                           (SELECT pg_partition_ancestors(c.oid))))"
    )
}

fn table_sql(table: &TableRef, publication: &str) -> String {
    let facts = relation_facts_sql(publication);
    format!(
        "SELECT json_build_object({facts},
            'partitions', COALESCE((
                SELECT json_agg(json_build_object({facts}))
                FROM pg_partition_tree(top.oid) tree
                JOIN pg_class c ON c.oid = tree.relid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE tree.relid <> top.oid AND tree.isleaf), '[]'::json))::text
         FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
         JOIN pg_class top ON top.oid = c.oid
         WHERE n.nspname = {} AND c.relname = {}",
        quote_literal(&table.database),
        quote_literal(&table.table)
    )
}

fn single_json<T: serde::de::DeserializeOwned>(
    rows: Vec<Vec<Option<String>>>,
) -> Result<Option<T>> {
    match rows
        .into_iter()
        .next()
        .and_then(|row| row.into_iter().next().flatten())
    {
        Some(text) => Ok(Some(serde_json::from_str(&text)?)),
        None => Ok(None),
    }
}

pub(crate) fn evaluate_server(
    server: &ServerFacts,
    stream: &PostgresStreamOptions,
    slot_exists: bool,
    report: &mut GateReport,
) {
    let (major, minor) = stream.min_server_version;
    if server.version_num < major * 10_000 + minor {
        report.fail(
            "server_version",
            format!(
                "{} is below the required {major}.{minor}",
                server.version_num
            ),
            Some("upgrade the server"),
        );
    }
    if server.wal_level != "logical" {
        report.fail(
            "wal_level",
            format!("{} (required logical)", server.wal_level),
            Some("ALTER SYSTEM SET wal_level = logical; then restart the server"),
        );
    }
    if !server.can_replicate {
        report.fail(
            "account",
            "the account has no REPLICATION attribute",
            Some("ALTER ROLE <account> REPLICATION"),
        );
    }
    if !slot_exists && server.slots_used >= server.max_replication_slots {
        report.fail(
            "max_replication_slots",
            format!(
                "all {} are in use and the slot does not exist",
                server.max_replication_slots
            ),
            Some("raise max_replication_slots, or drop a slot that is not needed"),
        );
    }
    if server.senders_used >= server.max_wal_senders {
        report.fail(
            "max_wal_senders",
            format!("all {} are in use", server.max_wal_senders),
            Some("raise max_wal_senders; then restart the server"),
        );
    }
    if server.encoding == "SQL_ASCII" {
        report.fail(
            "database encoding",
            "SQL_ASCII declares no encoding, and one byte that is not UTF-8 ends the stream",
            Some("capture from a database with a real encoding"),
        );
    }
    if server.max_slot_wal_keep_size == "-1" {
        report.warn(
            "max_slot_wal_keep_size",
            "unlimited: a stopped consumer can fill the WAL disk",
            Some("set a limit the disk can hold"),
        );
    }
    if let Some(timeout) = &server.idle_replication_slot_timeout {
        if timeout != "0" && under_an_hour(timeout) {
            report.warn(
                "idle_replication_slot_timeout",
                format!("{timeout}: a stopped consumer's slot is invalidated within the hour"),
                Some("set it to at least 1h, or to 0"),
            );
        }
    }
    if server.in_recovery {
        if server.hot_standby_feedback != "on" {
            report.fail(
                "hot_standby_feedback",
                format!(
                    "{} (required on, capturing from a standby)",
                    server.hot_standby_feedback
                ),
                Some("ALTER SYSTEM SET hot_standby_feedback = on; SELECT pg_reload_conf()"),
            );
        }
        if !slot_exists {
            report.warn(
                "standby",
                "creating a slot on a standby waits until the primary logs a snapshot",
                Some("run SELECT pg_log_standby_snapshot() on the primary"),
            );
        }
    }
}

/// `current_setting` prints the setting with its unit: `30s`, `10min`, `1h`, `2d`.
fn under_an_hour(setting: &str) -> bool {
    let digits = setting.trim_end_matches(|c: char| !c.is_ascii_digit());
    let Ok(n) = digits.parse::<u64>() else {
        return false;
    };
    match &setting[digits.len()..] {
        "ms" => n < 3_600_000,
        "s" => n < 3_600,
        "min" | "" => n < 60,
        _ => false,
    }
}

pub(crate) fn evaluate_publication(
    name: &str,
    publication: Option<&PublicationFacts>,
    report: &mut GateReport,
) {
    let object = format!("publication {name}");
    let Some(publication) = publication else {
        report.fail(
            object,
            "does not exist in this database",
            Some("CREATE PUBLICATION in the database being captured"),
        );
        return;
    };
    let missing: Vec<&str> = [
        ("insert", publication.insert),
        ("update", publication.update),
        ("delete", publication.delete),
        ("truncate", publication.truncate),
    ]
    .into_iter()
    .filter(|(_, on)| !on)
    .map(|(op, _)| op)
    .collect();
    if !missing.is_empty() {
        report.fail(
            object.clone(),
            format!("does not publish {}", missing.join(", ")),
            Some("ALTER PUBLICATION ... SET (publish = 'insert, update, delete, truncate')"),
        );
    }
    if publication.via_root {
        report.fail(
            object,
            "publish_via_partition_root is on: a partition's truncate would send nothing",
            Some("ALTER PUBLICATION ... SET (publish_via_partition_root = false)"),
        );
    }
}

fn evaluate_relation(
    relation: &RelationFacts,
    publication: &str,
    publication_generated: Option<&str>,
    report: &mut GateReport,
) {
    let object = relation.name.as_str();
    match relation.relkind.as_str() {
        "r" | "p" => {}
        "f" => report.fail(object, "a foreign table writes no WAL here", None),
        other => report.fail(object, format!("relkind {other} is not a table"), None),
    }
    match relation.relpersistence.as_str() {
        "p" => {}
        "u" => report.fail(
            object,
            "UNLOGGED: its changes are not in the WAL",
            Some("ALTER TABLE ... SET LOGGED"),
        ),
        _ => report.fail(object, "a temporary table is not in the WAL", None),
    }
    if relation.relreplident != "f" {
        report.fail(
            object,
            "REPLICA IDENTITY is not FULL",
            Some("ALTER TABLE ... REPLICA IDENTITY FULL"),
        );
    }
    // A partitioned parent is published through its leaves.
    if relation.relkind != "p" && !relation.in_publication {
        report.fail(
            object,
            format!("not in publication {publication}"),
            Some("ALTER PUBLICATION ... ADD TABLE"),
        );
    }
    if relation.has_row_filter {
        report.fail(
            object,
            format!("has a row filter in publication {publication}"),
            Some("ALTER PUBLICATION ... SET TABLE without WHERE"),
        );
    }
    if relation.has_column_list {
        report.fail(
            object,
            format!("has a column list in publication {publication}"),
            Some("ALTER PUBLICATION ... SET TABLE without a column list"),
        );
    }
    if relation.generated_columns > 0 && publication_generated != Some("s") {
        report.fail(
            object,
            "has generated columns the publication does not send",
            Some("on 18: ALTER PUBLICATION ... SET (publish_generated_columns = stored)"),
        );
    }
}

pub(crate) fn evaluate_table(
    table: &TableRef,
    relation: Option<&RelationFacts>,
    publication: &str,
    publication_generated: Option<&str>,
    report: &mut GateReport,
) {
    let Some(relation) = relation else {
        report.fail(table.as_subject(), "no such table", None);
        return;
    };
    evaluate_relation(relation, publication, publication_generated, report);
    if !relation.has_primary_key {
        report.fail(
            relation.name.as_str(),
            "has no primary key",
            Some("ALTER TABLE ... ADD PRIMARY KEY"),
        );
    }
    for partition in &relation.partitions {
        evaluate_relation(partition, publication, publication_generated, report);
    }
}

pub async fn validate_capture_gates(
    options: &PostgresOptions,
    stream: &PostgresStreamOptions,
    tables: &[TableRef],
) -> Result<GateReport> {
    let mut conn = Connection::connect(options, false).await?;
    let mut report = GateReport::default();
    let server: ServerFacts = single_json(conn.query(SERVER_SQL).await?)?
        .ok_or_else(|| ce_stream_core::error::Error::Source("server facts: no row".into()))?;
    let slot_exists = crate::slots::read_figures(&mut conn, &stream.slot)
        .await?
        .is_some();
    evaluate_server(&server, stream, slot_exists, &mut report);
    let publication: Option<PublicationFacts> =
        single_json(conn.query(&publication_sql(&stream.publication)).await?)?;
    evaluate_publication(&stream.publication, publication.as_ref(), &mut report);
    let generated = publication.as_ref().and_then(|p| p.generated.as_deref());
    for table in tables {
        let relation: Option<RelationFacts> =
            single_json(conn.query(&table_sql(table, &stream.publication)).await?)?;
        evaluate_table(
            table,
            relation.as_ref(),
            &stream.publication,
            generated,
            &mut report,
        );
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server() -> ServerFacts {
        ServerFacts {
            version_num: 170_005,
            wal_level: "logical".into(),
            in_recovery: false,
            max_replication_slots: 10,
            slots_used: 1,
            max_wal_senders: 10,
            senders_used: 1,
            max_slot_wal_keep_size: "10GB".into(),
            hot_standby_feedback: "off".into(),
            idle_replication_slot_timeout: None,
            encoding: "UTF8".into(),
            can_replicate: true,
        }
    }

    fn stream() -> PostgresStreamOptions {
        PostgresStreamOptions {
            slot: "s".into(),
            ..Default::default()
        }
    }

    fn table() -> RelationFacts {
        RelationFacts {
            name: "public.orders".into(),
            relkind: "r".into(),
            relpersistence: "p".into(),
            relreplident: "f".into(),
            has_primary_key: true,
            generated_columns: 0,
            in_publication: true,
            has_row_filter: false,
            has_column_list: false,
            partitions: Vec::new(),
        }
    }

    fn server_report(facts: &ServerFacts) -> GateReport {
        let mut report = GateReport::default();
        evaluate_server(facts, &stream(), true, &mut report);
        report
    }

    fn table_report(relation: &RelationFacts, generated: Option<&str>) -> GateReport {
        let mut report = GateReport::default();
        evaluate_table(
            &TableRef::new("public", "orders"),
            Some(relation),
            "ce_stream",
            generated,
            &mut report,
        );
        report
    }

    fn says(report: &GateReport, text: &str) -> bool {
        report
            .failures
            .iter()
            .any(|item| item.to_string().contains(text))
    }

    #[test]
    fn a_good_server_and_table_pass() {
        let report = server_report(&server());
        assert!(report.passed() && report.warnings.is_empty(), "{report:?}");
        assert!(table_report(&table(), None).passed());
    }

    #[test]
    fn server_failures_name_the_setting_and_the_fix() {
        let mut facts = server();
        facts.version_num = 160_004;
        facts.wal_level = "replica".into();
        facts.encoding = "SQL_ASCII".into();
        facts.can_replicate = false;
        facts.senders_used = 10;
        let report = server_report(&facts);
        assert_eq!(report.failures.len(), 5);
        assert!(says(&report, "160004 is below the required 17.0"));
        assert!(says(
            &report,
            "wal_level: replica (required logical). Remediation: ALTER SYSTEM"
        ));
        assert!(says(&report, "SQL_ASCII"));
        assert!(says(&report, "REPLICATION"));
        assert!(says(&report, "max_wal_senders"));
    }

    #[test]
    fn the_embedders_floor_is_an_option() {
        let mut floor = stream();
        floor.min_server_version = (17, 5);
        let mut facts = server();
        facts.version_num = 170_004;
        let mut report = GateReport::default();
        evaluate_server(&facts, &floor, true, &mut report);
        assert!(says(&report, "below the required 17.5"));
        facts.version_num = 180_000;
        let mut report = GateReport::default();
        evaluate_server(&facts, &floor, true, &mut report);
        assert!(report.passed());
    }

    #[test]
    fn a_free_slot_is_needed_only_when_the_slot_does_not_exist() {
        let mut facts = server();
        facts.slots_used = 10;
        assert!(server_report(&facts).passed());
        let mut report = GateReport::default();
        evaluate_server(&facts, &stream(), false, &mut report);
        assert!(says(&report, "max_replication_slots"));
    }

    #[test]
    fn warnings_do_not_stop_capture() {
        let mut facts = server();
        facts.max_slot_wal_keep_size = "-1".into();
        facts.idle_replication_slot_timeout = Some("10min".into());
        let report = server_report(&facts);
        assert!(report.passed());
        assert_eq!(report.warnings.len(), 2);
        facts.idle_replication_slot_timeout = Some("2h".into());
        facts.max_slot_wal_keep_size = "1GB".into();
        assert!(server_report(&facts).warnings.is_empty());
        facts.idle_replication_slot_timeout = Some("0".into());
        assert!(server_report(&facts).warnings.is_empty());
    }

    #[test]
    fn a_standby_needs_feedback_and_says_how_a_slot_gets_made() {
        let mut facts = server();
        facts.in_recovery = true;
        let mut report = GateReport::default();
        evaluate_server(&facts, &stream(), false, &mut report);
        assert!(says(&report, "hot_standby_feedback"));
        assert!(report.warnings[0]
            .to_string()
            .contains("pg_log_standby_snapshot"));
    }

    #[test]
    fn publication_shape() {
        let good = PublicationFacts {
            insert: true,
            update: true,
            delete: true,
            truncate: true,
            via_root: false,
            generated: None,
        };
        let mut report = GateReport::default();
        evaluate_publication("p", Some(&good), &mut report);
        assert!(report.passed());
        let mut report = GateReport::default();
        evaluate_publication("p", None, &mut report);
        assert!(says(&report, "publication p: does not exist"));
        let mut report = GateReport::default();
        evaluate_publication(
            "p",
            Some(&PublicationFacts {
                truncate: false,
                delete: false,
                via_root: true,
                ..good
            }),
            &mut report,
        );
        assert!(says(&report, "does not publish delete, truncate"));
        assert!(says(&report, "publish_via_partition_root"));
    }

    #[test]
    fn table_failures() {
        let mut bad = table();
        bad.relpersistence = "u".into();
        bad.relreplident = "d".into();
        bad.has_primary_key = false;
        bad.in_publication = false;
        bad.has_row_filter = true;
        bad.has_column_list = true;
        let report = table_report(&bad, None);
        assert_eq!(report.failures.len(), 6);
        for text in [
            "UNLOGGED",
            "REPLICA IDENTITY",
            "primary key",
            "not in publication ce_stream",
            "row filter",
            "column list",
        ] {
            assert!(says(&report, text), "{text}");
        }
        let mut foreign = table();
        foreign.relkind = "f".into();
        assert!(says(&table_report(&foreign, None), "foreign"));
        let mut report = GateReport::default();
        evaluate_table(
            &TableRef::new("public", "gone"),
            None,
            "ce_stream",
            None,
            &mut report,
        );
        assert!(says(&report, "public.gone: no such table"));
    }

    #[test]
    fn generated_columns_need_the_publication_to_send_them() {
        let mut facts = table();
        facts.generated_columns = 1;
        assert!(says(&table_report(&facts, None), "generated columns"));
        assert!(says(&table_report(&facts, Some("n")), "generated columns"));
        assert!(table_report(&facts, Some("s")).passed());
    }

    #[test]
    fn every_leaf_of_a_parent_is_checked_and_the_parent_is_published_through_them() {
        let mut parent = table();
        parent.relkind = "p".into();
        parent.in_publication = false;
        let mut leaf = table();
        leaf.name = "public.orders_1".into();
        leaf.relreplident = "d".into();
        parent.partitions = vec![leaf];
        let report = table_report(&parent, None);
        assert_eq!(report.failures.len(), 1);
        assert!(says(&report, "public.orders_1: REPLICA IDENTITY"));
    }
}

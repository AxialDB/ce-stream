//! Against a server, ignored by default. `CE_STREAM_PG_PORT` names a cluster with
//! `wal_level = logical`, the superuser `postgres` and the role `axialdb_cdc` of
//! `CE_STREAM_PG_PASSWORD` and `CE_STREAM_PG_CDC_PASSWORD`, and `CE_STREAM_PSQL`
//! its psql. Objects are named `ce19_live_*`.
//!
//! ```text
//! cargo test -p ce-stream-postgres --test live -- --ignored --test-threads=1
//! ```

use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ce_stream_core::delivery::{CheckpointMode, Delivery, SeedCut};
use ce_stream_core::error::Error;
use ce_stream_core::event::TableRef;
use ce_stream_core::source::{ChangeSource, DeliveryUnit, SourceConfig};
use ce_stream_core::transaction::{ControlKind, SourcePosition};
use ce_stream_postgres::{
    catalog_tables, create_slot, drop_slot, new_confirm, slot_figures, validate_capture_gates, Lsn,
    PgSeedCut, PostgresOptions, PostgresSource, PostgresStreamOptions,
};

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is not set"))
}

fn psql_command() -> Command {
    let mut command = Command::new(env("CE_STREAM_PSQL"));
    command
        .env("PGPASSWORD", env("CE_STREAM_PG_PASSWORD"))
        .args([
            "-X",
            "-q",
            "-At",
            "-v",
            "ON_ERROR_STOP=1",
            "-h",
            "127.0.0.1",
        ])
        .args([
            "-p",
            &env("CE_STREAM_PG_PORT"),
            "-U",
            "postgres",
            "-d",
            "axialdb_test",
        ]);
    command
}

fn psql(sql: &str) -> String {
    let out = psql_command().args(["-c", sql]).output().expect("psql");
    assert!(
        out.status.success(),
        "{sql}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn psql_in_background(sql: &str) -> Child {
    psql_command()
        .args(["-c", sql])
        .stdout(Stdio::null())
        .spawn()
        .expect("psql")
}

fn superuser() -> PostgresOptions {
    PostgresOptions {
        host: "127.0.0.1".into(),
        port: env("CE_STREAM_PG_PORT").parse().unwrap(),
        user: "postgres".into(),
        password: env("CE_STREAM_PG_PASSWORD"),
        database: "axialdb_test".into(),
        tls: false,
        tls_ca: None,
    }
}

fn stream_account() -> PostgresOptions {
    PostgresOptions {
        user: "axialdb_cdc".into(),
        password: env("CE_STREAM_PG_CDC_PASSWORD"),
        ..superuser()
    }
}

/// A fresh table, publication and slot; `name` keeps tests apart.
async fn fresh(name: &str) -> PostgresStreamOptions {
    let table = format!("ce19_live_{name}");
    let slot = format!("ce19_live_{name}");
    let _ = drop_slot(&superuser(), &slot).await;
    psql(&format!(
        "DROP PUBLICATION IF EXISTS {table}; DROP TABLE IF EXISTS {table};
         CREATE TABLE {table}(id int PRIMARY KEY, note text);
         ALTER TABLE {table} REPLICA IDENTITY FULL;
         CREATE PUBLICATION {table} FOR TABLE {table};"
    ));
    create_slot(&stream_account(), &slot).await.unwrap();
    PostgresStreamOptions {
        slot,
        publication: table,
        message_prefixes: vec!["ce19_live".into()],
        ..Default::default()
    }
}

fn source(name: &str, stream: PostgresStreamOptions, checkpoint: CheckpointMode) -> PostgresSource {
    PostgresSource {
        options: stream_account(),
        stream,
        config: SourceConfig {
            source_id: "postgres://lab/axialdb_test".into(),
            include_tables: vec![TableRef::new("public", format!("ce19_live_{name}"))],
            delivery_unit: DeliveryUnit::Transaction,
            checkpoint,
            ..Default::default()
        },
        checkpoint: None,
        checkpoint_store: None,
        skip_gate_check: false,
        include: Default::default(),
        confirm: new_confirm(),
    }
}

async fn cleanup(name: &str) {
    let table = format!("ce19_live_{name}");
    let _ = drop_slot(&superuser(), &table).await;
    psql(&format!(
        "DROP PUBLICATION IF EXISTS {table}; DROP TABLE IF EXISTS {table};"
    ));
}

/// Run the source until `done` says stop, or fail after a minute.
async fn stream_until(
    mut src: PostgresSource,
    mut done: impl FnMut(&Delivery) -> bool + Send + 'static,
) -> Vec<Delivery> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let keep = Arc::clone(&seen);
    let run = src.run_deliveries(move |delivery| {
        let stop = done(&delivery);
        keep.lock().unwrap().push(delivery);
        if stop {
            return Err(Error::Source("test is done".into()));
        }
        Ok(())
    });
    let result = tokio::time::timeout(Duration::from_secs(60), run)
        .await
        .expect("the stream delivered what was wanted within a minute");
    assert!(result.unwrap_err().to_string().contains("test is done"));
    let delivered = seen.lock().unwrap().clone();
    delivered
}

#[tokio::test]
#[ignore = "needs a server"]
async fn the_cut_waits_for_a_running_transaction_and_its_mark_comes_through_the_stream() {
    let name = "cut";
    let stream = fresh(name).await;
    let table = format!("ce19_live_{name}");
    let cut = PgSeedCut::new(superuser(), "ce19_live");

    // Running across the mark, with an xid, and committing after it.
    let mut writer = psql_in_background(&format!(
        "BEGIN; INSERT INTO {table} VALUES (1, 'across'); SELECT pg_sleep(4); COMMIT;"
    ));
    tokio::time::sleep(Duration::from_secs(1)).await;
    let mark = cut.mark().await.unwrap();
    let began = Instant::now();
    cut.wait_settled(&mark).await.unwrap();
    assert!(
        began.elapsed() > Duration::from_secs(2),
        "settled in {:?} with a transaction running",
        began.elapsed()
    );
    assert!(writer.wait().unwrap().success());
    // Settled: a snapshot now has that row, and a later wait returns at once.
    assert_eq!(psql(&format!("SELECT count(*) FROM {table}")), "1");
    let again = cut.mark().await.unwrap();
    let began = Instant::now();
    cut.wait_settled(&again).await.unwrap();
    assert!(began.elapsed() < Duration::from_secs(2));

    let src = source(name, stream, CheckpointMode::OnOk);
    let target = mark.clone();
    let delivered = stream_until(src, move |delivery| {
        matches!(delivery, Delivery::Commit(_))
            && ce_stream_postgres::covers_position(position_of(delivery), &target).unwrap()
    })
    .await;
    // The mark, as the stream has it, is the position mark() returned.
    let in_stream = delivered
        .iter()
        .find_map(|d| match d {
            Delivery::Progress { position } if position.at.get("content").is_some() => {
                Some(position)
            }
            _ => None,
        })
        .expect("the mark came through the stream");
    assert_eq!(in_stream.at, mark.at);
    assert_eq!(in_stream.after, mark.after);
    assert!(ce_stream_postgres::covers_position(in_stream, &mark).unwrap());
    // The row across the mark commits after it, and arrives after it.
    let commit_after_mark = delivered
        .iter()
        .position(|d| matches!(d, Delivery::Commit(_)))
        .unwrap();
    let mark_at = delivered
        .iter()
        .position(|d| matches!(d, Delivery::Progress { position } if position == in_stream))
        .unwrap();
    assert!(mark_at < commit_after_mark);
    cleanup(name).await;
}

fn position_of(delivery: &Delivery) -> &SourcePosition {
    match delivery {
        Delivery::Commit(txn) => txn.position.as_ref().unwrap(),
        Delivery::Progress { position } => position,
        Delivery::Schema(_) => panic!("a definition has no position"),
    }
}

#[tokio::test]
#[ignore = "needs a server"]
async fn the_slot_moves_only_when_a_position_is_confirmed() {
    let name = "ack";
    let stream = fresh(name).await;
    let table = format!("ce19_live_{name}");
    let slot = stream.slot.clone();
    let before = slot_figures(&superuser(), &slot)
        .await
        .unwrap()
        .unwrap()
        .confirmed_flush_lsn;
    psql(&format!("INSERT INTO {table} VALUES (1, 'a')"));

    // Delivered, not confirmed: well past a status update, the slot has not moved.
    let mut src = source(name, stream.clone(), CheckpointMode::OnConfirm);
    let confirm = src.confirm.clone();
    let delivered = Arc::new(Mutex::new(None));
    let keep = Arc::clone(&delivered);
    let run = src.run_deliveries(move |delivery| {
        if matches!(delivery, Delivery::Commit(_)) {
            *keep.lock().unwrap() = Some(position_of(&delivery).clone());
        }
        Ok(())
    });
    let held = async {
        tokio::time::sleep(Duration::from_secs(12)).await;
        let held = slot_figures(&superuser(), &slot)
            .await
            .unwrap()
            .unwrap()
            .confirmed_flush_lsn;
        assert_eq!(held, before, "the slot moved with nothing confirmed");
        let position = delivered.lock().unwrap().clone().expect("a commit");
        confirm.confirm(&position).unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await;
        let moved = slot_figures(&superuser(), &slot)
            .await
            .unwrap()
            .unwrap()
            .confirmed_flush_lsn;
        assert_eq!(
            moved.as_deref(),
            position.after["lsn"].as_str(),
            "a confirm moves the slot to its position"
        );
    };
    tokio::select! {
        _ = held => {}
        result = run => panic!("the stream ended: {result:?}"),
    }
    cleanup(name).await;
}

#[tokio::test]
#[ignore = "needs a server"]
async fn the_catalog_query_answers_and_its_signals_move() {
    let name = "cat";
    let stream = fresh(name).await;
    let table = format!("ce19_live_{name}");
    let parted = format!("{table}_p");
    psql(&format!(
        "DROP TABLE IF EXISTS {parted};
         CREATE TABLE {parted}(id int, r int, PRIMARY KEY(id, r)) PARTITION BY LIST(r);
         CREATE TABLE {parted}_1 PARTITION OF {parted} FOR VALUES IN (1);
         ALTER PUBLICATION {table} ADD TABLE {parted};"
    ));
    let oid = |name: &str| -> u32 {
        psql(&format!("SELECT '{name}'::regclass::oid"))
            .parse()
            .unwrap()
    };
    let (table_oid, parted_oid) = (oid(&table), oid(&parted));
    // The stream's account can ask too: the catalog is readable by anyone.
    let tables = catalog_tables(
        &stream_account(),
        &[table_oid, parted_oid, 1],
        &stream.publication,
    )
    .await
    .unwrap();
    assert_eq!(tables.len(), 2, "an OID with no table has no entry");
    let plain = tables.iter().find(|t| t.oid == table_oid).unwrap();
    assert_eq!(plain.relreplident, "f");
    assert_eq!(plain.relkind, "r");
    assert_eq!(plain.primary_key, ["id"]);
    assert!(plain.in_publication);
    assert_eq!(plain.columns[1].name, "note");
    let parent = tables.iter().find(|t| t.oid == parted_oid).unwrap();
    assert_eq!(parent.relkind, "p");
    assert_eq!(parent.primary_key, ["id", "r"]);
    assert_eq!(parent.partitions.len(), 1);
    let leaf = &parent.partitions[0];
    assert!(leaf.in_publication);
    assert!(leaf.inherits_xmin.is_some() && leaf.type_xmin.is_some());

    // A rewrite moves the file node and the column's row.
    psql(&format!("ALTER TABLE {table} ALTER COLUMN id TYPE bigint"));
    let after = catalog_tables(&stream_account(), &[table_oid], &stream.publication)
        .await
        .unwrap();
    assert_ne!(after[0].relfilenode, plain.relfilenode);
    assert_ne!(after[0].columns[0].xmin, plain.columns[0].xmin);
    assert_eq!(after[0].columns[0].atttypid, 20);
    psql(&format!("DROP TABLE {parted}"));
    cleanup(name).await;
}

#[tokio::test]
#[ignore = "needs a server"]
async fn the_gate_reads_a_real_server() {
    let name = "gate";
    let stream = fresh(name).await;
    let table = format!("ce19_live_{name}");
    let tables = [TableRef::new("public", table.clone())];
    let report = validate_capture_gates(&stream_account(), &stream, &tables)
        .await
        .unwrap();
    assert!(report.passed(), "{report:?}");

    psql(&format!("ALTER TABLE {table} REPLICA IDENTITY DEFAULT"));
    let report = validate_capture_gates(&stream_account(), &stream, &tables)
        .await
        .unwrap();
    assert!(!report.passed());
    assert!(report.failures[0].to_string().contains("REPLICA IDENTITY"));

    // The source refuses on it, naming the table.
    let mut src = source(name, stream.clone(), CheckpointMode::OnOk);
    let err = src.run_deliveries(|_| Ok(())).await.unwrap_err();
    assert!(err.to_string().contains(&table), "{err}");

    psql(&format!("ALTER TABLE {table} REPLICA IDENTITY FULL"));
    let mut missing = stream.clone();
    missing.publication = "ce19_live_no_such".into();
    let report = validate_capture_gates(&stream_account(), &missing, &tables)
        .await
        .unwrap();
    assert!(report
        .failures
        .iter()
        .any(|item| item.to_string().contains("does not exist")));
    let report = validate_capture_gates(
        &stream_account(),
        &stream,
        &[TableRef::new("public", "ce19_live_no_such")],
    )
    .await
    .unwrap();
    assert!(report.failures[0].to_string().contains("no such table"));
    cleanup(name).await;
}

#[tokio::test]
#[ignore = "needs a server"]
async fn a_checkpoint_behind_the_slot_is_history_lost_and_a_foreign_one_is_refused() {
    let name = "behind";
    let stream = fresh(name).await;
    let table = format!("ce19_live_{name}");
    psql(&format!("INSERT INTO {table} VALUES (1, 'a')"));
    let mut src = source(name, stream.clone(), CheckpointMode::OnOk);
    src.checkpoint = Some(ce_stream_core::Checkpoint {
        adapter: "postgres".into(),
        payload: serde_json::json!({ "lsn": "0/1" }),
    });
    let err = src.run_deliveries(|_| Ok(())).await.unwrap_err();
    assert!(matches!(err, Error::HistoryLost(_)), "{err}");

    let mut src = source(name, stream, CheckpointMode::OnOk);
    src.checkpoint = Some(ce_stream_core::Checkpoint {
        adapter: "postgres".into(),
        payload: serde_json::json!({ "lsn": "F/0", "system_id": "1", "timeline": "1" }),
    });
    let err = src.run_deliveries(|_| Ok(())).await.unwrap_err();
    assert!(err.to_string().contains("system"), "{err}");
    cleanup(name).await;
}

fn is_keepalive(delivery: &Delivery) -> bool {
    matches!(delivery, Delivery::Progress { position }
        if position.at.get("xid").is_none() && position.at.get("content").is_none())
}

fn lsn_at(_position: &SourcePosition, field: &serde_json::Value) -> Lsn {
    Lsn::parse(field["lsn"].as_str().unwrap()).unwrap()
}

fn row_id(event: &ce_stream_core::CloudEvent) -> i64 {
    event.data["after"]["id"].as_i64().unwrap()
}

#[tokio::test]
#[ignore = "needs a server"]
async fn a_keepalive_tells_a_quiet_stream_where_it_is_and_is_never_ahead_of_a_commit() {
    let name = "keep";
    let stream = fresh(name).await;
    let table = format!("ce19_live_{name}");
    let noise = format!("{table}_noise");
    psql(&format!(
        "DROP TABLE IF EXISTS {noise}; CREATE TABLE {noise}(id int)"
    ));

    // Nothing published is written: the stream sends nothing but its keepalives.
    psql(&format!("INSERT INTO {noise} VALUES (1)"));
    let target = Lsn::parse(&psql("SELECT pg_current_wal_lsn()::text")).unwrap();
    let src = source(name, stream.clone(), CheckpointMode::OnOk);
    let delivered = stream_until(src, move |delivery| {
        is_keepalive(delivery)
            && lsn_at(position_of(delivery), &position_of(delivery).after) >= target
    })
    .await;
    assert!(
        !delivered.iter().any(|d| matches!(d, Delivery::Commit(_))),
        "nothing for the consumer was written"
    );

    // Commits with unpublished writes between them, so keepalives come among them.
    const COMMITS: i64 = 60;
    let mut writer = psql_command();
    for id in 1..=COMMITS {
        writer
            .args(["-c", &format!("INSERT INTO {table} VALUES ({id}, 'x')")])
            .args(["-c", &format!("INSERT INTO {noise} VALUES ({id})")])
            .args(["-c", "SELECT pg_sleep(0.05)"]);
    }
    let mut writer = writer.stdout(Stdio::null()).spawn().expect("psql");
    let src = source(name, stream, CheckpointMode::OnOk);
    let delivered = stream_until(src, move |delivery| {
        matches!(delivery, Delivery::Commit(txn)
            if txn.events.iter().any(|event| row_id(event) == COMMITS))
    })
    .await;
    assert!(writer.wait().unwrap().success());
    let keepalives = delivered.iter().filter(|d| is_keepalive(d)).count();
    let mut furthest = Lsn(0);
    for delivery in &delivered {
        match delivery {
            Delivery::Commit(_) => {
                let position = position_of(delivery);
                let at = lsn_at(position, &position.at);
                assert!(
                    at >= furthest,
                    "a commit at {at} came after a keepalive at {furthest}"
                );
            }
            other if is_keepalive(other) => {
                let position = position_of(other);
                furthest = furthest.max(lsn_at(position, &position.after));
            }
            _ => {}
        }
    }
    eprintln!("{keepalives} keepalives among {COMMITS} commits");
    psql(&format!("DROP TABLE {noise}"));
    cleanup(name).await;
}

/// What an embedder reads to tell a rewrite: the file node, the column's row, and
/// whether the server announces the table again at the next change.
#[tokio::test]
#[ignore = "needs a server"]
async fn what_maintenance_shows_in_the_catalog_and_on_the_stream() {
    let name = "matrix";
    let stream = fresh(name).await;
    let table = format!("ce19_live_{name}");
    let oid: u32 = psql(&format!("SELECT '{table}'::regclass::oid"))
        .parse()
        .unwrap();
    let publication = stream.publication.clone();
    let catalog = || async {
        catalog_tables(&superuser(), &[oid], &publication)
            .await
            .unwrap()
            .remove(0)
    };
    psql(&format!("INSERT INTO {table} VALUES (0, 'first')"));

    // step, what it runs, moves the file node, moves the first column's row.
    let steps: &[(&str, &str, bool, bool)] = &[
        ("add column", "ADD", false, false),
        ("vacuum", "VACUUM {t}", false, false),
        ("vacuum full", "VACUUM FULL {t}", true, false),
        ("cluster", "CLUSTER {t} USING {t}_pkey", true, false),
        ("truncate", "TRUNCATE {t}", true, false),
        ("reindex", "REINDEX TABLE {t}", false, false),
        ("analyze", "ANALYZE {t}", false, false),
        ("alter type", "ALTER", true, true),
    ];
    let mut before = catalog().await;
    for (i, (step, sql, node_moves, column_moves)) in steps.iter().enumerate() {
        let sql = match *sql {
            "ADD" => format!("ALTER TABLE {table} ADD COLUMN extra{i} int"),
            "ALTER" => format!("ALTER TABLE {table} ALTER COLUMN id TYPE bigint"),
            other => other.replace("{t}", &table),
        };
        psql(&sql);
        psql(&format!(
            "INSERT INTO {table}(id, note) VALUES ({}, 'after')",
            i + 1
        ));
        let after = catalog().await;
        assert_eq!(
            after.relfilenode != before.relfilenode,
            *node_moves,
            "file node after {step}"
        );
        assert_eq!(
            after.columns[0].xmin != before.columns[0].xmin,
            *column_moves,
            "column row after {step}"
        );
        before = after;
    }

    // Partitions: attaching moves the set, and the new one has its own rows.
    let parted = format!("{table}_p");
    psql(&format!(
        "DROP TABLE IF EXISTS {parted}, {parted}_2;
         CREATE TABLE {parted}(id int, r int, PRIMARY KEY(id, r)) PARTITION BY LIST(r);
         CREATE TABLE {parted}_1 PARTITION OF {parted} FOR VALUES IN (1);
         CREATE TABLE {parted}_2(id int NOT NULL, r int NOT NULL);"
    ));
    let parent: u32 = psql(&format!("SELECT '{parted}'::regclass::oid"))
        .parse()
        .unwrap();
    let partitions = || async {
        catalog_tables(&superuser(), &[parent], &publication)
            .await
            .unwrap()
            .remove(0)
            .partitions
    };
    let one = partitions().await;
    assert_eq!(one.len(), 1);
    psql(&format!(
        "ALTER TABLE {parted} ATTACH PARTITION {parted}_2 FOR VALUES IN (2)"
    ));
    let two = partitions().await;
    assert_eq!(two.len(), 2);
    assert_eq!(one[0], two[0], "the first partition's rows have not moved");
    assert!(two[1].inherits_xmin.is_some());
    psql(&format!("ALTER TABLE {parted} DETACH PARTITION {parted}_2"));
    assert_eq!(partitions().await.len(), 1);
    psql(&format!("DROP TABLE {parted}, {parted}_2"));

    // The stream: which steps the server announces the table again after.
    let last = steps.len() as i64;
    let src = source(name, stream, CheckpointMode::OnOk);
    let delivered = stream_until(src, move |delivery| {
        matches!(delivery, Delivery::Commit(txn)
            if txn.events.iter().any(|event| row_id(event) == last))
    })
    .await;
    let mut announced = false;
    let mut after_step = Vec::new();
    for delivery in &delivered {
        match delivery {
            Delivery::Schema(_) => announced = true,
            Delivery::Commit(txn) if !txn.events.is_empty() => {
                let changed = txn
                    .control
                    .iter()
                    .any(|c| c.kind == ControlKind::DefinitionChanged);
                let id = row_id(&txn.events[0]);
                after_step.push((id, announced, changed));
                announced = false;
            }
            _ => {}
        }
    }
    for (id, announced, changed) in &after_step {
        let step = usize::try_from(*id - 1).ok().and_then(|i| steps.get(i));
        eprintln!(
            "{:<12} announced again: {announced:<5} definition changed: {changed}",
            step.map_or("first row", |step| step.0)
        );
    }
    let of = |id: i64| after_step.iter().find(|row| row.0 == id).copied().unwrap();
    assert_eq!(of(0), (0, true, false), "the first row has its definition");
    assert_eq!(of(1), (1, true, true), "a new column is a change");
    assert_eq!(of(last), (last, true, true), "a new type is a change");
    cleanup(name).await;
}

#[tokio::test]
#[ignore = "needs a server with max_prepared_transactions above 0"]
async fn a_prepared_transaction_is_delivered_at_commit_prepared_only() {
    if psql("SHOW max_prepared_transactions") == "0" {
        eprintln!("max_prepared_transactions = 0: not run");
        return;
    }
    let name = "prep";
    let stream = fresh(name).await;
    let table = format!("ce19_live_{name}");
    psql(&format!(
        "BEGIN; INSERT INTO {table} VALUES (1, 'kept'); PREPARE TRANSACTION 'ce19_live_a'"
    ));
    psql(&format!(
        "BEGIN; INSERT INTO {table} VALUES (2, 'dropped'); PREPARE TRANSACTION 'ce19_live_b'"
    ));
    psql(&format!("INSERT INTO {table} VALUES (3, 'plain')"));
    psql("COMMIT PREPARED 'ce19_live_a'");
    psql("ROLLBACK PREPARED 'ce19_live_b'");
    psql(&format!("INSERT INTO {table} VALUES (4, 'last')"));
    let src = source(name, stream, CheckpointMode::OnOk);
    let delivered = stream_until(src, |delivery| {
        matches!(delivery, Delivery::Commit(txn)
            if txn.events.iter().any(|event| row_id(event) == 4))
    })
    .await;
    let ids: Vec<i64> = delivered
        .iter()
        .filter_map(|d| match d {
            Delivery::Commit(txn) => Some(txn.events.iter().map(row_id).collect::<Vec<_>>()),
            _ => None,
        })
        .flatten()
        .collect();
    assert_eq!(
        ids,
        [3, 1, 4],
        "in commit order, and the rollback not at all"
    );
    cleanup(name).await;
}
